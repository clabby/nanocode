use std::{
    io::Write,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use futures_util::Stream;
use nanocode_platform::Instant;
use serde::Serialize;
use serde_json::value::to_raw_value;
use tokio::sync::mpsc;

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
}

impl AgentEvents {
    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub async fn recv(&mut self) -> Option<AgentEvent> {
        self.recv_timed().await.map(|event| event.event)
    }

    #[doc(hidden)]
    pub async fn recv_timed(&mut self) -> Option<TimedAgentEvent> {
        self.receiver.recv().await
    }

    pub fn poll_recv_timed(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<TimedAgentEvent>> {
        self.receiver.poll_recv(context)
    }

    #[doc(hidden)]
    pub fn try_recv_timed(&mut self) -> Option<TimedAgentEvent> {
        self.receiver.try_recv().ok()
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
        self.receiver
            .poll_recv(context)
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

#[derive(Clone)]
struct EventChannel {
    request_id: Arc<str>,
    next_seq: Arc<AtomicU64>,
    session: mpsc::UnboundedSender<TimedAgentEvent>,
    turn: Option<mpsc::UnboundedSender<TimedAgentEvent>>,
    turn_admission: Option<Arc<AtomicU64>>,
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
        let (session, receiver) = mpsc::unbounded_channel();
        (
            Self {
                request_id: Arc::clone(&request_id),
                next_seq: Arc::new(AtomicU64::new(1)),
                session,
                turn: None,
                turn_admission: None,
            },
            AgentEvents {
                request_id,
                receiver,
            },
        )
    }

    fn mirrored_channel(&self) -> (Self, AgentEvents) {
        let (turn, receiver) = mpsc::unbounded_channel();
        (
            Self {
                request_id: Arc::clone(&self.request_id),
                next_seq: Arc::clone(&self.next_seq),
                session: self.session.clone(),
                turn: Some(turn),
                turn_admission: Some(Arc::new(AtomicU64::new(0))),
            },
            AgentEvents {
                request_id: Arc::clone(&self.request_id),
                receiver,
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
        event: &AgentEvent,
    ) -> Result<Option<TurnAdmission<'_>>, EventError> {
        let terminal_claim = if event.kind.is_terminal() {
            self.claim_terminal()?
        } else {
            None
        };
        let admission = if terminal_claim.is_none() && !event.kind.is_terminal() {
            self.admit_non_terminal()?
        } else {
            None
        };
        self.reserve_exact_sequence(event.seq)?;
        if let Some(claim) = terminal_claim {
            claim.commit();
        }
        Ok(admission)
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
        drop(self.session.send(event.clone()));
        if let Some(turn) = &self.turn {
            drop(turn.send(event));
        }
    }
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

    pub fn publish(&self, event: AgentEvent) -> Result<(), EventError> {
        self.publish_timed(TimedAgentEvent {
            event,
            timing: AgentEventTiming {
                emitted_ns: monotonic_now_ns(),
                source_received_ns: None,
            },
        })
    }

    fn publish_timed(&self, event: TimedAgentEvent) -> Result<(), EventError> {
        self.validate(&event.event)?;
        let admission = self.channel.reserve_publisher_event(&event.event)?;
        self.channel.publish(event);
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
        if (kind == AgentEventKind::ApiEvent && !self.raw_api_events)
            || self.publisher.channel.receivers_are_closed()
        {
            let seq = self.publisher.channel.allocate_sequence()?;
            self.publisher.channel.record_local_terminal(kind);
            return Ok(seq);
        }

        let payload = if let Some(turn_id) = self
            .publisher
            .turn_id
            .as_deref()
            .filter(|_| kind != AgentEventKind::InputAccepted)
        {
            #[derive(Serialize)]
            struct TurnPayload<'a, P> {
                #[serde(flatten)]
                payload: &'a P,
                turn_id: &'a str,
            }
            Arc::from(
                to_raw_value(&TurnPayload {
                    payload: &payload,
                    turn_id,
                })
                .map_err(EventError::Encode)?,
            )
        } else {
            Arc::from(to_raw_value(&payload).map_err(EventError::Encode)?)
        };
        let seq = self.publisher.channel.allocate_sequence()?;
        let event = TimedAgentEvent {
            event: AgentEvent {
                protocol_version: AGENT_EVENT_PROTOCOL_VERSION,
                request_id: Arc::clone(&self.publisher.channel.request_id),
                seq,
                kind,
                payload,
            },
            timing: AgentEventTiming {
                emitted_ns: monotonic_now_ns(),
                source_received_ns,
            },
        };
        self.publisher.channel.publish(event);
        self.publisher.channel.record_local_terminal(kind);
        Ok(seq)
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
