use std::{
    collections::HashMap,
    ops::Range,
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
};

use nanocode_code_mode::{
    CodeModeToolset, ToolContext, ToolOutput, ToolOutputBody,
    code_mode::{CellJournalSink, CellJournalUpdate, CellJournals, CodeModeRuntime},
    embedded::{HostFuture, NestedToolCall, OwnedToolContext},
    sim::{Mutation as CodeModeMutation, mutate as mutate_code_mode, nested_overlap},
};
use nanocode_code_mode_engine::{CellTools, EmbeddedHost, Interrupt, RuntimeEvent};
use nanocode_sync::watch;
use nanocode_tool_runtime::{SessionEnvironment, Tools, runtime::ToolRuntime};
use serde_json::{Value, json};

use crate::{
    executor::Clock,
    rng::{FNV_OFFSET, Rng, fnv},
};

const CALLS_MIN: u64 = 1_000;
const CALLS_MAX: u64 = 30_000;
const BATCH_MAX: u64 = 500;
const PAD_MAX: u64 = 64;
const OBSERVER_YIELD_MS: u64 = 10;
const OBSERVER_SPINS_MAX: u64 = 100_000;
const OVERLAP_CALLS_MAX: u64 = 8;
const OVERLAP_TICKS: u64 = 8;
const OVERLAP_TICK_NS: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellCallsMutation {
    None,
    CrossResults,
    RootRecoveryUnbound,
    AdmissionHeld,
    AdmitsStale,
    ObserverSaveHeld,
    NestedEndsFirst,
    NestedStartsFirst,
}

impl CellCallsMutation {
    const ALL: [Self; 8] = [
        Self::None,
        Self::CrossResults,
        Self::RootRecoveryUnbound,
        Self::AdmissionHeld,
        Self::AdmitsStale,
        Self::ObserverSaveHeld,
        Self::NestedEndsFirst,
        Self::NestedStartsFirst,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|mutation| mutation.name() == name)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::CrossResults => "cross_results",
            Self::RootRecoveryUnbound => "root_recovery_unbound",
            Self::AdmissionHeld => "admission_held",
            Self::AdmitsStale => "admits_stale",
            Self::ObserverSaveHeld => "observer_save_held",
            Self::NestedEndsFirst => "nested_ends_first",
            Self::NestedStartsFirst => "nested_starts_first",
        }
    }

    pub fn names() -> String {
        Self::ALL[1..]
            .iter()
            .map(|mutation| mutation.name())
            .collect::<Vec<_>>()
            .join("|")
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CellCallsStats {
    pub seeds: u64,
    pub calls: u64,
    pub batches: u64,
}

impl CellCallsStats {
    pub const fn add(&mut self, other: &Self) {
        self.seeds += other.seeds;
        self.calls += other.calls;
        self.batches += other.batches;
    }
}

pub struct CellCallsOutcome {
    pub digest: u64,
    pub shape: String,
    pub stats: CellCallsStats,
    pub wrong: Option<String>,
}

fn block<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        std::thread::yield_now();
    }
}

fn drive(
    host: &mut EmbeddedHost,
    calls: u64,
    pad: &str,
    mutation: CellCallsMutation,
    digest: &mut u64,
) -> Result<(), String> {
    let mut seen = vec![false; usize::try_from(calls).expect("a call count fits in usize")];
    let mut received = 0_u64;
    let mut previous = 0_u64;

    loop {
        match block(host.read_event()).map_err(|error| format!("read_event: {error}"))? {
            RuntimeEvent::ToolCall { id, input, .. } => {
                let index = input.get("i").and_then(Value::as_u64).ok_or_else(|| {
                    format!("cell_call_once: call {id} carries no index: {input}")
                })?;
                let slot = usize::try_from(index)
                    .ok()
                    .and_then(|index| seen.get_mut(index))
                    .ok_or_else(|| {
                        format!("cell_call_once: call {id} names index {index} of {calls}")
                    })?;
                if *slot {
                    return Err(format!(
                        "cell_call_once: index {index} reached the host twice (call {id}, {received} received)"
                    ));
                }
                *slot = true;
                received += 1;
                *digest = fnv(*digest, &index.to_le_bytes());

                let answer = if mutation == CellCallsMutation::CrossResults && received > 1 {
                    previous
                } else {
                    index
                };
                previous = index;
                let encoded = json!({ "i": answer, "p": pad }).to_string();
                host.send_tool_result(1, id, encoded, true)
                    .map_err(|error| format!("send_tool_result: {error}"))?;
            }
            RuntimeEvent::Done { stored, .. } => {
                if received != calls {
                    return Err(format!(
                        "cell_call_once: {received} of {calls} calls reached the host"
                    ));
                }

                let expected = json!([calls, calls * (calls - 1) / 2]);
                let reported = stored
                    .get("done")
                    .and_then(|raw| serde_json::from_str::<Value>(raw.get()).ok());
                if reported.as_ref() != Some(&expected) {
                    return Err(format!(
                        "cell_call_once: the cell reported {reported:?}, not {expected}"
                    ));
                }
                return Ok(());
            }
            RuntimeEvent::Error { message, .. } => {
                return Err(format!("cell failed after {received} calls: {message}"));
            }
            _ => {}
        }
    }
}

fn root_recovery(seed: u64, mutation: CellCallsMutation) -> Result<(), String> {
    let root = format!("cell-calls-{seed}");
    let child = format!("{root}.child");

    for session in [&root, &child] {
        let journals = CellJournals::new();
        let tools = Tools::builder()
            .without_defaults()
            .cell_journals(journals.clone())
            .build()
            .map_err(|error| format!("tools: {error}"))?;
        let tools = if mutation == CellCallsMutation::RootRecoveryUnbound {
            tools
        } else {
            tools.for_session(&SessionEnvironment::new(session, &root))
        };
        drop(ToolRuntime::new_with_tools(
            std::env::temp_dir(),
            Vec::new(),
            &tools,
        ));

        let preempts = journals.preempts_recovery(session);
        if preempts != (*session == root) {
            return Err(format!(
                "root_recovery_preemptible: session {session} of root {root} lets a steer cut its recovered observation short: {preempts}"
            ));
        }
    }
    Ok(())
}

struct StalledJournals {
    gate: watch::Sender<bool>,
    saves: Arc<AtomicU64>,
}

impl CellJournalSink for StalledJournals {
    fn save(&self, _update: CellJournalUpdate) -> HostFuture<'static, Result<(), String>> {
        self.saves.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }

    fn forget(&self, _session_id: &str, _cell_id: u64) -> HostFuture<'static, ()> {
        Box::pin(async {})
    }

    fn load(
        &self,
        _session_id: &str,
    ) -> HostFuture<'static, Result<Vec<nanocode_code_mode::code_mode::CellJournal>, String>> {
        let mut gate = self.gate.subscribe();
        Box::pin(async move {
            gate.wait_for(|open| *open)
                .await
                .map_err(|_| "the journal gate closed".to_owned())?;
            Ok(Vec::new())
        })
    }
}

struct NoTools;

impl CodeModeToolset for NoTools {
    fn nested_tool_metadata(&self) -> Vec<Value> {
        Vec::new()
    }

    fn execute_nested<'a>(
        &'a self,
        name: &'a str,
        _input: Value,
        _context: ToolContext<'a>,
    ) -> HostFuture<'a, ToolOutput> {
        panic!("the admission check's cell has no tool {name}")
    }
}

const fn code_mode_mutation(mutation: CellCallsMutation) -> CodeModeMutation {
    match mutation {
        CellCallsMutation::AdmissionHeld => CodeModeMutation::AdmissionHeld,
        CellCallsMutation::AdmitsStale => CodeModeMutation::AdmitsStale,
        CellCallsMutation::ObserverSaveHeld => CodeModeMutation::ObserverSaveHeld,
        CellCallsMutation::NestedEndsFirst => CodeModeMutation::NestedEndsFirst,
        CellCallsMutation::NestedStartsFirst => CodeModeMutation::NestedStartsFirst,
        _ => CodeModeMutation::None,
    }
}

struct GatedSaves {
    gate: watch::Sender<bool>,
}

impl CellJournalSink for GatedSaves {
    fn save(&self, _update: CellJournalUpdate) -> HostFuture<'static, Result<(), String>> {
        let mut gate = self.gate.subscribe();
        Box::pin(async move {
            gate.wait_for(|open| *open)
                .await
                .map_err(|_| "the journal gate closed".to_owned())?;
            Ok(())
        })
    }

    fn forget(&self, _session_id: &str, _cell_id: u64) -> HostFuture<'static, ()> {
        Box::pin(async {})
    }
}

struct PendingTool;

impl CodeModeToolset for PendingTool {
    fn nested_tool_metadata(&self) -> Vec<Value> {
        vec![json!({ "kind": "function", "name": "t", "tool_name": "t" })]
    }

    fn execute_nested<'a>(
        &'a self,
        _name: &'a str,
        _input: Value,
        _context: ToolContext<'a>,
    ) -> HostFuture<'a, ToolOutput> {
        Box::pin(std::future::pending())
    }
}

fn cell_context(session: &str, call_id: &str) -> OwnedToolContext {
    OwnedToolContext {
        instruction_revision: None,
        model: String::new(),
        session_id: session.to_owned(),
        call_id: call_id.to_owned(),
        turn_id: None,
        history: Arc::default(),
        output_token_budget: nanocode_code_mode::contract::DEFAULT_TOOL_OUTPUT_TOKENS,
        host_context: None,
        tool_settings: Default::default(),
    }
}

fn spin<T>(
    future: &mut std::pin::Pin<&mut impl Future<Output = T>>,
    spawned: &mut Vec<nanocode_platform::HostFuture>,
    clock: &Clock,
) -> Option<T> {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..OBSERVER_SPINS_MAX {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return Some(value);
        }
        spawned.extend(crate::host::spawned().into_iter().map(|(future, _)| future));
        spawned.retain_mut(|future| future.as_mut().poll(&mut cx).is_pending());
        if !clock.advance_to_next() {
            std::thread::yield_now();
        }
    }
    None
}

fn observer_save(seed: u64, mutation: CellCallsMutation, clock: &Clock) -> Result<(), String> {
    mutate_code_mode(code_mode_mutation(mutation));
    let session = format!("cell-observer-{seed}");
    let gate = watch::channel(true).0;
    let mut runtime = CodeModeRuntime::new_with_turn(Arc::new(AtomicU64::new(1)));
    runtime.set_journals(CellJournals::with_sink(Arc::new(GatedSaves {
        gate: gate.clone(),
    })));
    let mut spawned = Vec::new();
    let source =
        format!("// @exec: {{\"yield_time_ms\": {OBSERVER_YIELD_MS}}}\nawait tools.t({{}});");
    let exec = runtime.execute(
        &source,
        Arc::new(PendingTool),
        cell_context(&session, &format!("{session}-exec")),
    );
    let mut exec = std::pin::pin!(exec);
    let execution = spin(&mut exec, &mut spawned, clock)
        .ok_or_else(|| "observer_save: the exec never yielded".to_owned())?;
    if !execution.cell.is_some_and(|cell| cell.running) {
        return Err(
            "observer_save: a cell waiting on a tool that never answers stopped".to_owned(),
        );
    }
    gate.send_replace(false);
    let wait = format!("{{\"cell_id\": \"1\", \"yield_time_ms\": {OBSERVER_YIELD_MS}}}");
    let first_context = cell_context(&session, &format!("{session}-wait-1"));
    let mut first = std::pin::pin!(runtime.wait(&wait, first_context.as_context()));
    let first_done = spin(&mut first, &mut spawned, clock);
    if let Some(execution) = first_done {
        return Err(format!(
            "observer_save: a wait answered while its journal saves were stalled (running {:?})",
            execution.cell.map(|cell| cell.running)
        ));
    }
    let second_context = cell_context(&session, &format!("{session}-wait-2"));
    let mut second = std::pin::pin!(runtime.wait(&wait, second_context.as_context()));
    let refused = spin(&mut second, &mut spawned, clock)
        .is_some_and(|execution| !execution.success && execution.cell.is_none());
    gate.send_replace(true);
    if refused {
        return Err(
            "observer_unheld: a wait was refused as busy because a wait that had passed its yield kept the cell's observer claim while its journal save was stalled"
                .to_owned(),
        );
    }
    Ok(())
}

fn nested_call(start: u64, ticks: u64) -> NestedToolCall {
    NestedToolCall {
        call_id: String::new(),
        name: "t".to_owned(),
        input: Value::Null,
        output: ToolOutputBody::Text(String::new()),
        structured_result: Value::Null,
        success: true,
        started_after_ns: start * OVERLAP_TICK_NS,
        duration_ns: ticks * OVERLAP_TICK_NS,
        metadata: None,
    }
}

fn nested_concurrency(seed: u64, mutation: CellCallsMutation) -> Result<(), String> {
    mutate_code_mode(code_mode_mutation(mutation));
    let mut rng = Rng::derive(seed, "cell_calls nested overlap");
    let count = rng.range(1, OVERLAP_CALLS_MAX);
    let spans: Vec<(u64, u64)> = (0..count)
        .map(|_| (rng.below(OVERLAP_TICKS), rng.below(OVERLAP_TICKS / 2)))
        .collect();
    let calls: Vec<NestedToolCall> = spans
        .iter()
        .map(|&(start, ticks)| nested_call(start, ticks))
        .collect();
    let running_at = |tick: u64| {
        spans
            .iter()
            .filter(|&&(start, ticks)| start <= tick && tick < start + ticks.max(1))
            .count()
    };
    let max_concurrent = spans
        .iter()
        .map(|&(start, _)| running_at(start))
        .max()
        .expect("a nested overlap check has at least one call");
    let busy_ticks = (0..OVERLAP_TICKS * 2)
        .filter(|&tick| {
            spans
                .iter()
                .any(|&(start, ticks)| start <= tick && tick < start + ticks)
        })
        .count();
    let expected = (
        u64::try_from(busy_ticks).expect("a tick count fits in u64") * OVERLAP_TICK_NS,
        u64::try_from(max_concurrent).expect("a call count fits in u64"),
    );
    let measured = nested_overlap(&calls);
    if measured != expected {
        return Err(format!(
            "nested_overlap: calls (start, ticks) {spans:?} measured (busy_ns, max_concurrent) {measured:?}, expected {expected:?}"
        ));
    }
    Ok(())
}

fn admission(seed: u64, mutation: CellCallsMutation) -> Result<(), String> {
    mutate_code_mode(code_mode_mutation(mutation));
    let session = format!("cell-admission-{seed}");
    let gate = watch::channel(false).0;
    let saves = Arc::new(AtomicU64::new(0));
    let mut runtime = CodeModeRuntime::new_with_turn(Arc::new(AtomicU64::new(1)));
    runtime.set_journals(CellJournals::with_sink(Arc::new(StalledJournals {
        gate: gate.clone(),
        saves: Arc::clone(&saves),
    })));
    let control = runtime.control();
    let context = OwnedToolContext {
        instruction_revision: None,
        model: String::new(),
        session_id: session.clone(),
        call_id: format!("{session}-exec"),
        turn_id: None,
        history: Arc::default(),
        output_token_budget: nanocode_code_mode::contract::DEFAULT_TOOL_OUTPUT_TOKENS,
        host_context: None,
        tool_settings: Default::default(),
    };
    let mut cx = Context::from_waker(Waker::noop());
    let mut exec = std::pin::pin!(runtime.execute("store('x', 1)", Arc::new(NoTools), context));
    if exec.as_mut().poll(&mut cx).is_ready() {
        return Err("admission: the exec finished while its journal load was stalled".to_owned());
    }
    let mut terminate = std::pin::pin!(control.terminate_turn(1, None));
    if terminate.as_mut().poll(&mut cx).is_pending() {
        return Err(
            "admission_unblocked: a terminate waited on another exec's stalled journal load"
                .to_owned(),
        );
    }
    gate.send_replace(true);
    let mut spawned = Vec::new();
    let execution = loop {
        if let Poll::Ready(execution) = exec.as_mut().poll(&mut cx) {
            break execution;
        }
        spawned.extend(crate::host::spawned().into_iter().map(|(future, _)| future));
        spawned.retain_mut(|future| future.as_mut().poll(&mut cx).is_pending());
        if saves.load(Ordering::SeqCst) != 0 || !control.quiet() {
            return Err(
                "admission_fenced: an exec admitted before a terminate moved the epoch started a cell"
                    .to_owned(),
            );
        }
        std::thread::yield_now();
    };
    let saved = saves.load(Ordering::SeqCst);
    if execution.cell.is_some() || saved != 0 || !control.quiet() {
        return Err(format!(
            "admission_fenced: an exec admitted before a terminate moved the epoch left a cell (cell {:?}, {saved} journal saves, quiet {})",
            execution.cell.map(|cell| cell.running),
            control.quiet()
        ));
    }
    Ok(())
}

fn simulate(seed: u64, mutation: CellCallsMutation) -> CellCallsOutcome {
    let mut rng = Rng::derive(seed, "cell_calls");
    let calls = rng.range(CALLS_MIN, CALLS_MAX);
    let batch = rng.range(1, BATCH_MAX);
    let pad = "x".repeat(usize::try_from(rng.below(PAD_MAX)).expect("a pad fits in usize"));
    let shape = format!("calls={calls} batch={batch} pad={}", pad.len());

    let code = format!(
        "const n = {calls}, b = {batch}; let c = 0, s = 0; for (let i = 0; i < n; i += b) {{ const r = await Promise.all(Array.from({{ length: Math.min(b, n - i) }}, (_, j) => tools.t({{ i: i + j }}))); for (const v of r) {{ const o = typeof v === 'string' ? JSON.parse(v) : v; c += 1; s += o.i; }} }} store('done', [c, s]);"
    );
    let tools = CellTools::encode(&[json!({ "kind": "function", "name": "t", "tool_name": "t" })])
        .expect("the cell tool encodes");
    let admitted = crate::host::entered(seed, &Clock::new(), || admission(seed, mutation));
    let observed = {
        let clock = Clock::new();
        crate::host::entered(seed, &clock, || observer_save(seed, mutation, &clock))
    };
    let overlapped = nested_concurrency(seed, mutation);
    crate::host::enter(seed, &Clock::new());
    let mut host = EmbeddedHost::spawn().expect("the embedded host starts");
    let mut digest = fnv(FNV_OFFSET, shape.as_bytes());

    let wrong = admitted
        .and_then(|()| observed)
        .and_then(|()| overlapped)
        .and_then(|()| root_recovery(seed, mutation))
        .and_then(|()| {
            host.start_cell(
                1,
                &code,
                HashMap::new(),
                Arc::new(tools),
                None,
                None,
                Interrupt::new(),
            )
            .map_err(|error| format!("start_cell: {error}"))
        })
        .and_then(|()| drive(&mut host, calls, &pad, mutation, &mut digest))
        .err();

    CellCallsOutcome {
        digest,
        shape,
        stats: CellCallsStats {
            seeds: 1,
            calls,
            batches: calls.div_ceil(batch),
        },
        wrong,
    }
}

pub fn judge(
    seed: u64,
    mutation: CellCallsMutation,
    twice: bool,
    verbose: bool,
    totals: &mut CellCallsStats,
) -> Option<String> {
    let first = simulate(seed, mutation);
    totals.add(&first.stats);
    if verbose {
        println!("seed {seed} digest {:016x} {}", first.digest, first.shape);
    }
    if first.wrong.is_some() || !twice {
        return first
            .wrong
            .map(|message| format!("{} :: {message}", first.shape));
    }
    let second = simulate(seed, mutation);
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "{} :: nondeterminism: trace digest {:016x} then {:016x}",
            first.shape, first.digest, second.digest
        )
    })
}

pub fn replay(seed: u64, mutation: CellCallsMutation) -> String {
    let mut command = format!("nanocode-sim --scenario cell_calls --seed {seed}");
    if mutation != CellCallsMutation::None {
        command.push_str(" --mutation ");
        command.push_str(mutation.name());
    }
    command
}

pub fn report(
    seeds: &Range<u64>,
    passed: u64,
    failed: u64,
    twice: bool,
    mutation: CellCallsMutation,
    totals: &CellCallsStats,
) -> ExitCode {
    println!(
        "RESULT seeds={}..{} scenario=cell_calls mutation={} passed={passed} failed={failed} twice={twice} stats={totals:?}",
        seeds.start,
        seeds.end,
        mutation.name()
    );
    match mutation {
        CellCallsMutation::None if failed == 0 => ExitCode::SUCCESS,
        CellCallsMutation::None => ExitCode::FAILURE,
        _ if failed > 0 => {
            println!("MUTATION {} caught by {failed} seeds", mutation.name());
            ExitCode::SUCCESS
        }
        _ => {
            println!("MUTATION {} NOT caught", mutation.name());
            ExitCode::FAILURE
        }
    }
}
