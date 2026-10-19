use std::{
    borrow::Cow,
    io::Write,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};

use arc_swap::ArcSwap;
use futures_util::Stream;
use nanocode_platform::Instant;
use nanocode_types_core::responses::MessagePhase;
use serde::Serialize;
use serde_json::value::{RawValue, to_raw_value};
use tokio::sync::mpsc;

use nanocode_types_core::events::TextDeltaRef;
pub use nanocode_types_core::events::{
    AGENT_EVENT_PROTOCOL_VERSION, AgentEvent, AgentEventKind, EventError,
};
static PROCESS_MONOTONIC_EPOCH: OnceLock<Instant> = OnceLock::new();

#[doc(hidden)]
#[must_use]
pub fn monotonic_now_ns() -> u64 {
    let elapsed = PROCESS_MONOTONIC_EPOCH
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos();
    u64::try_from(elapsed).unwrap_or(u64::MAX)
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct AgentEventTiming {
    pub emitted_ns: u64,

    pub source_received_ns: Option<u64>,
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct TimedAgentEvent {
    pub event: AgentEvent,

    pub timing: AgentEventTiming,
}

pub struct AgentEvents {
    request_id: Arc<str>,
    receiver: mpsc::UnboundedReceiver<TimedAgentEvent>,
    unread: Option<Arc<AtomicU64>>,
}

impl AgentEvents {
    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub async fn recv(&mut self) -> Option<AgentEvent> {
        self.recv_timed().await.map(|event| event.event)
    }

    fn received(&self, event: Option<TimedAgentEvent>) -> Option<TimedAgentEvent> {
        if event.is_some()
            && let Some(unread) = &self.unread
        {
            unread.fetch_sub(1, Ordering::Relaxed);
        }
        event
    }

    #[doc(hidden)]
    pub async fn recv_timed(&mut self) -> Option<TimedAgentEvent> {
        let event = self.receiver.recv().await;
        self.received(event)
    }

    pub fn poll_recv_timed(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<TimedAgentEvent>> {
        self.receiver
            .poll_recv(context)
            .map(|event| self.received(event))
    }

    #[doc(hidden)]
    pub fn try_recv_timed(&mut self) -> Option<TimedAgentEvent> {
        let event = self.receiver.try_recv().ok();
        self.received(event)
    }

    pub async fn write_jsonl(mut self, mut output: impl Write) -> Result<(), EventError> {
        while let Some(event) = self.recv().await {
            write_jsonl_event(&mut output, &event)?;
        }
        Ok(())
    }

    pub async fn write_turn_jsonl(&mut self, mut output: impl Write) -> Result<(), EventError> {
        while let Some(event) = self.recv().await {
            let terminal = event.kind.is_terminal();
            write_jsonl_event(&mut output, &event)?;
            if terminal {
                return Ok(());
            }
        }
        Err(EventError::ClosedBeforeTerminal)
    }
}

impl Stream for AgentEvents {
    type Item = AgentEvent;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.poll_recv_timed(context)
            .map(|event| event.map(|event| event.event))
    }
}

fn write_jsonl_event(output: &mut impl Write, event: &AgentEvent) -> Result<(), EventError> {
    serde_json::to_writer(&mut *output, event).map_err(EventError::Encode)?;
    output
        .write_all(b"\n")
        .and_then(|()| output.flush())
        .map_err(EventError::Write)
}

const UNREAD_WARNING: u64 = 1024;

#[derive(Clone)]
struct Subscriber {
    sender: mpsc::UnboundedSender<TimedAgentEvent>,
    unread: Arc<AtomicU64>,
}

impl Subscriber {
    fn channel(request_id: &Arc<str>) -> (Self, AgentEvents) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let unread = Arc::new(AtomicU64::new(0));
        (
            Self {
                sender,
                unread: Arc::clone(&unread),
            },
            AgentEvents {
                request_id: Arc::clone(request_id),
                receiver,
                unread: Some(unread),
            },
        )
    }
}

struct Subscribers(ArcSwap<Vec<Subscriber>>);

impl Subscribers {
    fn channel(request_id: &Arc<str>) -> (Arc<Self>, AgentEvents) {
        let (subscriber, events) = Subscriber::channel(request_id);
        (
            Arc::new(Self(ArcSwap::from_pointee(vec![subscriber]))),
            events,
        )
    }

    fn subscribe(&self, request_id: &Arc<str>) -> AgentEvents {
        let (subscriber, events) = Subscriber::channel(request_id);
        self.0.rcu(|subscribers| {
            subscribers
                .iter()
                .filter(|subscriber| !subscriber.sender.is_closed())
                .cloned()
                .chain(std::iter::once(subscriber.clone()))
                .collect::<Vec<_>>()
        });
        events
    }

    fn is_closed(&self) -> bool {
        self.0
            .load()
            .iter()
            .all(|subscriber| subscriber.sender.is_closed())
    }

    fn send(&self, event: &TimedAgentEvent) {
        let mut closed = false;
        for subscriber in self.0.load().iter() {
            let unread = subscriber.unread.fetch_add(1, Ordering::Relaxed) + 1;
            if subscriber.sender.send(event.clone()).is_err() {
                subscriber.unread.fetch_sub(1, Ordering::Relaxed);
                closed = true;
                continue;
            }
            if unread >= UNREAD_WARNING && unread.is_power_of_two() {
                tracing::warn!(
                    request_id = %event.event.request_id,
                    unread,
                    "an AgentEvents stream is not being read; drain it or drop it"
                );
            }
        }
        if closed {
            self.0.rcu(|subscribers| {
                subscribers
                    .iter()
                    .filter(|subscriber| !subscriber.sender.is_closed())
                    .cloned()
                    .collect::<Vec<_>>()
            });
        }
    }
}

#[derive(Clone)]
struct EventChannel {
    request_id: Arc<str>,
    next_seq: Arc<AtomicU64>,
    session: Arc<Subscribers>,
    turn: Option<mpsc::UnboundedSender<TimedAgentEvent>>,
    turn_admission: Option<Arc<AtomicU64>>,
    pending: Arc<Mutex<PendingDelta>>,
}

struct DeltaKey {
    kind: AgentEventKind,
    turn_id: Option<Arc<str>>,
    model_call_index: u32,
    phase: Option<MessagePhase>,
    model_elapsed_ns: Option<u64>,
    source_received_ns: Option<u64>,
}

#[derive(Default)]
struct PendingDelta {
    key: Option<DeltaKey>,
    item_id: Option<String>,
    text: String,
}

impl PendingDelta {
    fn extends(
        &self,
        kind: AgentEventKind,
        turn_id: Option<&Arc<str>>,
        delta: &TextDeltaRef<'_>,
    ) -> bool {
        self.key.as_ref().is_some_and(|key| {
            key.kind == kind
                && key.turn_id.as_ref() == turn_id
                && key.model_call_index == delta.model_call_index
                && key.phase == delta.phase
        }) && self.item_id.as_deref() == delta.item_id.as_deref()
    }

    fn start(
        &mut self,
        kind: AgentEventKind,
        turn_id: Option<&Arc<str>>,
        delta: &TextDeltaRef<'_>,
        source_received_ns: Option<u64>,
    ) {
        self.key = Some(DeltaKey {
            kind,
            turn_id: turn_id.cloned(),
            model_call_index: delta.model_call_index,
            phase: delta.phase,
            model_elapsed_ns: delta.model_elapsed_ns,
            source_received_ns,
        });
        if self.item_id.as_deref() != delta.item_id.as_deref() {
            self.item_id = delta.item_id.as_deref().map(str::to_owned);
        }
        self.text.clear();
        self.text.push_str(&delta.text);
    }
}

const TURN_COMMITTED: u64 = 1 << 63;
const TURN_CLAIMING: u64 = 1 << 62;
const TURN_ADMISSIONS: u64 = TURN_CLAIMING - 1;
const TERMINAL_SPINS_BEFORE_YIELD: usize = 64;

struct TurnAdmission<'a> {
    state: &'a AtomicU64,
}

impl Drop for TurnAdmission<'_> {
    fn drop(&mut self) {
        self.state.fetch_sub(1, Ordering::Release);
    }
}

struct TerminalClaim<'a> {
    state: &'a AtomicU64,
    committed: bool,
}

impl TerminalClaim<'_> {
    fn commit(mut self) {
        self.state.fetch_or(TURN_COMMITTED, Ordering::Release);
        self.state.fetch_and(!TURN_CLAIMING, Ordering::Release);
        self.committed = true;
    }
}

impl Drop for TerminalClaim<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.state.fetch_and(!TURN_CLAIMING, Ordering::Release);
        }
    }
}

impl EventChannel {
    fn channel(request_id: Arc<str>) -> (Self, AgentEvents) {
        let (session, events) = Subscribers::channel(&request_id);
        (
            Self {
                request_id,
                next_seq: Arc::new(AtomicU64::new(1)),
                session,
                turn: None,
                turn_admission: None,
                pending: Arc::default(),
            },
            events,
        )
    }

    fn mirrored_channel(&self) -> (Self, AgentEvents) {
        let (turn, receiver) = mpsc::unbounded_channel();
        (
            Self {
                request_id: Arc::clone(&self.request_id),
                next_seq: Arc::clone(&self.next_seq),
                session: Arc::clone(&self.session),
                turn: Some(turn),
                turn_admission: Some(Arc::new(AtomicU64::new(0))),
                pending: Arc::default(),
            },
            AgentEvents {
                request_id: Arc::clone(&self.request_id),
                receiver,
                unread: None,
            },
        )
    }

    fn turn_is_terminal(&self) -> bool {
        self.turn_admission
            .as_ref()
            .is_some_and(|state| state.load(Ordering::Acquire) & TURN_COMMITTED != 0)
    }

    fn admit_non_terminal(&self) -> Result<Option<TurnAdmission<'_>>, EventError> {
        let Some(state) = self.turn_admission.as_deref() else {
            return Ok(None);
        };
        let mut current = state.load(Ordering::Acquire);
        loop {
            if current & TURN_COMMITTED != 0 {
                return Err(EventError::TurnAlreadyTerminal);
            }
            if current & TURN_CLAIMING != 0 {
                wait_for_terminal_claim(state, current);
                current = state.load(Ordering::Acquire);
                continue;
            }
            if current & TURN_ADMISSIONS == TURN_ADMISSIONS {
                return Err(EventError::SequenceExhausted);
            }
            match state.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(Some(TurnAdmission { state })),
                Err(observed) => current = observed,
            }
        }
    }

    fn claim_terminal(&self) -> Result<Option<TerminalClaim<'_>>, EventError> {
        let Some(state) = self.turn_admission.as_deref() else {
            return Ok(None);
        };
        let mut current = state.load(Ordering::Acquire);
        loop {
            if current & TURN_COMMITTED != 0 {
                return Err(EventError::TurnAlreadyTerminal);
            }
            if current & TURN_CLAIMING != 0 {
                wait_for_terminal_claim(state, current);
                current = state.load(Ordering::Acquire);
                continue;
            }
            match state.compare_exchange_weak(
                current,
                current | TURN_CLAIMING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        let mut spins = 0;
        while state.load(Ordering::Acquire) & TURN_ADMISSIONS != 0 {
            if spins < TERMINAL_SPINS_BEFORE_YIELD {
                spins += 1;
                std::hint::spin_loop();
            } else {
                spins = 0;
                std::thread::yield_now();
            }
        }
        Ok(Some(TerminalClaim {
            state,
            committed: false,
        }))
    }

    fn reserve_publisher_event(
        &self,
        kind: AgentEventKind,
        seq: Option<u64>,
    ) -> Result<(u64, Option<TurnAdmission<'_>>), EventError> {
        let terminal_claim = if kind.is_terminal() {
            self.claim_terminal()?
        } else {
            None
        };
        let admission = if terminal_claim.is_none() && !kind.is_terminal() {
            self.admit_non_terminal()?
        } else {
            None
        };
        let seq = match seq {
            Some(seq) => {
                self.reserve_exact_sequence(seq)?;
                seq
            }
            None => self.allocate_sequence()?,
        };
        if let Some(claim) = terminal_claim {
            claim.commit();
        }
        Ok((seq, admission))
    }

    fn reserve_exact_sequence(&self, seq: u64) -> Result<(), EventError> {
        let next_seq = seq.checked_add(1).ok_or(EventError::SequenceExhausted)?;
        self.next_seq
            .compare_exchange(seq, next_seq, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|expected| EventError::SequenceMismatch {
                expected,
                actual: seq,
            })
    }

    fn allocate_sequence(&self) -> Result<u64, EventError> {
        self.next_seq
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |seq| {
                seq.checked_add(1)
            })
            .map_err(|_| EventError::SequenceExhausted)
    }

    fn record_local_terminal(&self, kind: AgentEventKind) {
        if kind.is_terminal()
            && let Some(state) = &self.turn_admission
        {
            state.fetch_or(TURN_COMMITTED, Ordering::Release);
        }
    }

    fn receivers_are_closed(&self) -> bool {
        self.session.is_closed()
            && self
                .turn
                .as_ref()
                .is_none_or(mpsc::UnboundedSender::is_closed)
    }

    fn publish(&self, event: TimedAgentEvent) {
        self.session.send(&event);
        if let Some(turn) = &self.turn {
            drop(turn.send(event));
        }
    }

    fn lock_pending(&self) -> MutexGuard<'_, PendingDelta> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn publish_pending(&self, pending: &mut PendingDelta) -> Result<(), EventError> {
        let Some(key) = pending.key.take() else {
            return Ok(());
        };
        if self.receivers_are_closed() {
            return Ok(());
        }
        let payload = encode_payload(
            key.kind,
            key.turn_id.as_deref(),
            &TextDeltaRef {
                model_call_index: key.model_call_index,
                item_id: pending.item_id.as_deref().map(Cow::Borrowed),
                phase: key.phase,
                text: Cow::Borrowed(&pending.text),
                model_elapsed_ns: key.model_elapsed_ns,
            },
        )?;
        let seq = self.allocate_sequence()?;
        self.publish(TimedAgentEvent {
            event: AgentEvent {
                protocol_version: AGENT_EVENT_PROTOCOL_VERSION,
                request_id: Arc::clone(&self.request_id),
                seq,
                kind: key.kind,
                payload,
            },
            timing: AgentEventTiming {
                emitted_ns: monotonic_now_ns(),
                source_received_ns: key.source_received_ns,
            },
        });
        Ok(())
    }

    fn emit_text_delta(
        &self,
        kind: AgentEventKind,
        turn_id: Option<&Arc<str>>,
        delta: &TextDeltaRef<'_>,
        source_received_ns: Option<u64>,
    ) -> Result<(), EventError> {
        if self.receivers_are_closed() {
            return Ok(());
        }
        let mut pending = self.lock_pending();
        if pending.extends(kind, turn_id, delta) {
            pending.text.push_str(&delta.text);
            return Ok(());
        }
        self.publish_pending(&mut pending)?;
        pending.start(kind, turn_id, delta, source_received_ns);
        Ok(())
    }

    fn flush_deltas(&self) -> Result<(), EventError> {
        self.publish_pending(&mut self.lock_pending())
    }
}

fn encode_payload<P: Serialize>(
    kind: AgentEventKind,
    turn_id: Option<&str>,
    payload: &P,
) -> Result<Arc<RawValue>, EventError> {
    let encoded = if let Some(turn_id) = turn_id.filter(|_| kind != AgentEventKind::InputAccepted) {
        #[derive(Serialize)]
        struct TurnPayload<'a, P> {
            #[serde(flatten)]
            payload: &'a P,
            turn_id: &'a str,
        }
        to_raw_value(&TurnPayload { payload, turn_id })
    } else {
        to_raw_value(payload)
    };
    encoded.map(Arc::from).map_err(EventError::Encode)
}

fn wait_for_terminal_claim(state: &AtomicU64, observed: u64) {
    let mut spins = 0;
    while state.load(Ordering::Acquire) == observed {
        if spins < TERMINAL_SPINS_BEFORE_YIELD {
            spins += 1;
            std::hint::spin_loop();
        } else {
            spins = 0;
            std::thread::yield_now();
        }
    }
}

#[derive(Clone)]
pub struct AgentEventPublisher {
    channel: EventChannel,
    turn_id: Option<Arc<str>>,
}

impl AgentEventPublisher {
    #[must_use]
    pub fn with_turn_id(mut self, id: impl Into<Arc<str>>) -> Self {
        self.turn_id = Some(id.into());
        self
    }

    #[must_use]
    pub fn turn_id(&self) -> Option<&str> {
        self.turn_id.as_deref()
    }

    #[must_use]
    pub fn channel(request_id: impl Into<Arc<str>>) -> (Self, AgentEvents) {
        let request_id = request_id.into();
        let (channel, events) = EventChannel::channel(request_id);
        (
            Self {
                channel,
                turn_id: None,
            },
            events,
        )
    }

    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.channel.request_id
    }

    #[doc(hidden)]
    #[must_use]
    pub fn turn_is_terminal(&self) -> bool {
        self.channel.turn_is_terminal()
    }

    #[must_use]
    pub fn mirrored_channel(&self) -> (Self, AgentEvents) {
        let (channel, events) = self.channel.mirrored_channel();
        (
            Self {
                channel,
                turn_id: self.turn_id.clone(),
            },
            events,
        )
    }

    #[must_use]
    pub fn has_receivers(&self) -> bool {
        !self.channel.receivers_are_closed()
    }

    #[must_use]
    pub fn subscribe(&self) -> AgentEvents {
        self.channel.session.subscribe(&self.channel.request_id)
    }

    #[must_use]
    pub fn downgrade(&self) -> WeakAgentEventPublisher {
        WeakAgentEventPublisher {
            request_id: Arc::clone(&self.channel.request_id),
            next_seq: Arc::clone(&self.channel.next_seq),
            session: Arc::downgrade(&self.channel.session),
            turn: self
                .channel
                .turn
                .as_ref()
                .map(mpsc::UnboundedSender::downgrade),
            turn_admission: self.channel.turn_admission.clone(),
            pending: Arc::clone(&self.channel.pending),
            turn_id: self.turn_id.clone(),
        }
    }

    pub fn publish(&self, event: AgentEvent) -> Result<(), EventError> {
        self.publish_timed(TimedAgentEvent {
            event,
            timing: AgentEventTiming {
                emitted_ns: monotonic_now_ns(),
                source_received_ns: None,
            },
        })
    }

    pub fn publish_next(
        &self,
        kind: AgentEventKind,
        payload: Arc<RawValue>,
    ) -> Result<u64, EventError> {
        let mut pending = self.channel.lock_pending();
        self.channel.publish_pending(&mut pending)?;
        let (seq, admission) = self.channel.reserve_publisher_event(kind, None)?;
        if !self.channel.receivers_are_closed() {
            self.channel.publish(TimedAgentEvent {
                event: AgentEvent {
                    protocol_version: AGENT_EVENT_PROTOCOL_VERSION,
                    request_id: Arc::clone(&self.channel.request_id),
                    seq,
                    kind,
                    payload,
                },
                timing: AgentEventTiming {
                    emitted_ns: monotonic_now_ns(),
                    source_received_ns: None,
                },
            });
        }
        drop(admission);
        Ok(seq)
    }

    pub fn emit_text_delta(
        &self,
        kind: AgentEventKind,
        delta: TextDeltaRef<'_>,
        source_received_ns: Option<u64>,
    ) -> Result<(), EventError> {
        self.channel
            .emit_text_delta(kind, self.turn_id.as_ref(), &delta, source_received_ns)
    }

    pub fn flush_deltas(&self) -> Result<(), EventError> {
        self.channel.flush_deltas()
    }

    fn publish_timed(&self, event: TimedAgentEvent) -> Result<(), EventError> {
        self.validate(&event.event)?;
        let mut pending = self.channel.lock_pending();
        self.channel.publish_pending(&mut pending)?;
        let (_, admission) = self
            .channel
            .reserve_publisher_event(event.event.kind, Some(event.event.seq))?;
        if !self.channel.receivers_are_closed() {
            self.channel.publish(event);
        }
        drop(admission);
        Ok(())
    }

    fn validate(&self, event: &AgentEvent) -> Result<(), EventError> {
        if event.protocol_version != AGENT_EVENT_PROTOCOL_VERSION {
            return Err(EventError::ProtocolVersionMismatch {
                expected: AGENT_EVENT_PROTOCOL_VERSION,
                actual: event.protocol_version,
            });
        }
        if event.request_id != self.channel.request_id {
            return Err(EventError::RequestIdMismatch {
                expected: Arc::clone(&self.channel.request_id),
                actual: Arc::clone(&event.request_id),
            });
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct WeakAgentEventPublisher {
    request_id: Arc<str>,
    next_seq: Arc<AtomicU64>,
    session: std::sync::Weak<Subscribers>,
    turn: Option<mpsc::WeakUnboundedSender<TimedAgentEvent>>,
    turn_admission: Option<Arc<AtomicU64>>,
    pending: Arc<Mutex<PendingDelta>>,
    turn_id: Option<Arc<str>>,
}

impl WeakAgentEventPublisher {
    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    #[must_use]
    pub fn upgrade(&self) -> Option<AgentEventPublisher> {
        let turn = match &self.turn {
            Some(turn) => Some(turn.upgrade()?),
            None => None,
        };
        Some(AgentEventPublisher {
            channel: EventChannel {
                request_id: Arc::clone(&self.request_id),
                next_seq: Arc::clone(&self.next_seq),
                session: self.session.upgrade()?,
                turn,
                turn_admission: self.turn_admission.clone(),
                pending: Arc::clone(&self.pending),
            },
            turn_id: self.turn_id.clone(),
        })
    }
}

#[derive(Clone)]
pub struct EventSink {
    publisher: AgentEventPublisher,
    raw_api_events: bool,
}

impl EventSink {
    #[must_use]
    pub fn turn_id(&self) -> Option<&str> {
        self.publisher.turn_id()
    }

    #[must_use]
    pub fn channel(request_id: String) -> (Self, AgentEvents) {
        let (publisher, events) = AgentEventPublisher::channel(request_id);
        (Self::from_publisher(publisher), events)
    }

    #[must_use]
    pub fn request_id(&self) -> &str {
        self.publisher.request_id()
    }

    #[must_use]
    pub fn mirrored_channel(&self) -> (Self, AgentEvents) {
        let (publisher, events) = self.publisher.mirrored_channel();
        (
            Self {
                publisher,
                raw_api_events: self.raw_api_events,
            },
            events,
        )
    }

    pub fn emit<P: Serialize>(&self, kind: AgentEventKind, payload: P) -> Result<(), EventError> {
        self.emit_with_sequence(kind, payload).map(|_| ())
    }

    pub fn emit_with_sequence<P: Serialize>(
        &self,
        kind: AgentEventKind,
        payload: P,
    ) -> Result<u64, EventError> {
        self.emit_with_source_sequence(kind, payload, None)
    }

    pub fn emit_with_source_sequence<P: Serialize>(
        &self,
        kind: AgentEventKind,
        payload: P,
        source_received_ns: Option<u64>,
    ) -> Result<u64, EventError> {
        let channel = &self.publisher.channel;
        if (kind == AgentEventKind::ApiEvent && !self.raw_api_events)
            || channel.receivers_are_closed()
        {
            let seq = channel.allocate_sequence()?;
            channel.record_local_terminal(kind);
            return Ok(seq);
        }

        let payload = encode_payload(kind, self.publisher.turn_id.as_deref(), &payload)?;
        let mut pending = channel.lock_pending();
        channel.publish_pending(&mut pending)?;
        let seq = channel.allocate_sequence()?;
        channel.publish(TimedAgentEvent {
            event: AgentEvent {
                protocol_version: AGENT_EVENT_PROTOCOL_VERSION,
                request_id: Arc::clone(&channel.request_id),
                seq,
                kind,
                payload,
            },
            timing: AgentEventTiming {
                emitted_ns: monotonic_now_ns(),
                source_received_ns,
            },
        });
        channel.record_local_terminal(kind);
        drop(pending);
        Ok(seq)
    }

    pub fn emit_text_delta(
        &self,
        kind: AgentEventKind,
        delta: TextDeltaRef<'_>,
        source_received_ns: Option<u64>,
    ) -> Result<(), EventError> {
        self.publisher
            .emit_text_delta(kind, delta, source_received_ns)
    }

    pub fn flush_deltas(&self) -> Result<(), EventError> {
        self.publisher.flush_deltas()
    }

    #[must_use]
    pub fn has_receivers(&self) -> bool {
        self.publisher.has_receivers()
    }

    #[must_use]
    pub const fn from_publisher(publisher: AgentEventPublisher) -> Self {
        Self {
            publisher,
            raw_api_events: true,
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn with_raw_api_events(mut self, enabled: bool) -> Self {
        self.raw_api_events = enabled;
        self
    }

    #[must_use]
    pub fn publisher(&self) -> AgentEventPublisher {
        self.publisher.clone()
    }
}
