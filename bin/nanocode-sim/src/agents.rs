use std::{cell::RefCell, sync::Arc, time::Duration};

use nanocode::tool_contract::{Tool, ToolCollection, ToolContext, ToolInput};
use nanocode_agent_core::{AgentHandle, events::AgentEventKind};
use nanocode_board::{Board, NewTask, Op, Phase, TaskId};
use nanocode_code_mode::{
    CodeModeToolset, ToolOutput,
    code_mode::{CodeModeObserver, CodeModeRuntime},
    embedded::{HostFuture, OwnedToolContext},
};
use nanocode_dispatch::{Budgets, IntentState, Policy, Role};
use nanocode_dispatcher::{BRIEF, Dispatcher, Pool};
use nanocode_subagents::{
    AgentId, AgentStatus, AgentTask, Coordination, DEFAULT_MAX_SUBAGENTS, DISPATCH_LABEL,
    DISPATCH_POLL_MS, DISPATCH_ROLE, HostedView, MessagePriority, MessagePurpose, Registry,
    ScopedAgentUpdate, SubagentControl, SubagentPool,
};
use nanocode_sync::mpsc::UnboundedReceiver;
use nanocode_turnlog::{LogHost, MemoryLog};
use serde_json::Value;

use crate::{
    board::identity,
    executor::{Clock, Ended, Executor, Task},
    rng::Rng,
    scripted::{Counts, Ledger, Script, ScriptedFactory, ScriptedStore, simulated_model},
};

pub const CHILDREN_MIN: u64 = 1;
pub const CHILDREN_MAX: u64 = 64;
pub const RESIDENT_MAX: u64 = 4;
pub const TURN_STEPS_MAX: u64 = 8;
pub const STEP_LATENCY_MS_MIN: u64 = 50;
pub const STEP_LATENCY_MS_MAX: u64 = 20_000;
pub const RESTORE_LATENCY_MS_MAX: u64 = 2_000;
pub const SHUTDOWN_LATENCY_MS_MAX: u64 = 1_000;
pub const CANCEL_PPM: u64 = 250_000;
pub const POOLED_MAX: u64 = 4;
pub const RUNTIME_STOP_TIMEOUT: Duration = Duration::from_secs(30);
const ROOT_SESSION: &str = "root";
const POOL_SCOPE: &str = "pool";
const POOL_OWNER: &str = "agent:sim";
const POOL_POSTER: &str = "agent:sim+poster";
const POOL_KIND: &str = "kind";
const POOL_WORK: &str = "code";
const POOL_MEMBER: &str = "member";
const POOL_PROJECT: &str = "project";
const POOL_CHARTER_MS: u64 = 1_000;
const POOL_LEASE_MS: u64 = 3_600_000;
const POOL_IDLE_RETIRE_MS: u64 = 60_000;
const POOL_PASSES_MAX: u64 = 16;
const HOSTED_PASSES_MAX: u64 = 24;
const HOSTED_POLL: Duration = Duration::from_millis(DISPATCH_POLL_MS);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentsMutation {
    None,
    CancelIgnored,
    ShutdownHangs,
    ShutdownKeepsHarness,
    TurnLeaks,
    BoardFailuresUnread,
    PoolRetireKeepsAgent,
    PoolForgetsIntent,
    WaitRepeatsIds,
    WaitRepeatsUnread,
    WaitIgnoresCarrierRelease,
    WaitKeepsOriginTurn,
    WaitForgetsDiscard,
    RestoreRemembersWaits,
    RouteSkipsClaim,
    ScopeJobsOverlap,
    RestoreSharesClaim,
    ScopeJobsStopInline,
    ScopeLocksKept,
}

impl AgentsMutation {
    const ALL: [Self; 19] = [
        Self::None,
        Self::CancelIgnored,
        Self::ShutdownHangs,
        Self::ShutdownKeepsHarness,
        Self::TurnLeaks,
        Self::BoardFailuresUnread,
        Self::PoolRetireKeepsAgent,
        Self::PoolForgetsIntent,
        Self::WaitRepeatsIds,
        Self::WaitRepeatsUnread,
        Self::WaitIgnoresCarrierRelease,
        Self::WaitKeepsOriginTurn,
        Self::WaitForgetsDiscard,
        Self::RestoreRemembersWaits,
        Self::RouteSkipsClaim,
        Self::ScopeJobsOverlap,
        Self::RestoreSharesClaim,
        Self::ScopeJobsStopInline,
        Self::ScopeLocksKept,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|mutation| mutation.name() == name)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::CancelIgnored => "cancel_ignored",
            Self::ShutdownHangs => "shutdown_hangs",
            Self::ShutdownKeepsHarness => "shutdown_keeps_harness",
            Self::TurnLeaks => "turn_leaks",
            Self::BoardFailuresUnread => "board_failures_unread",
            Self::PoolRetireKeepsAgent => "pool_retire_keeps_agent",
            Self::PoolForgetsIntent => "pool_forgets_intent",
            Self::WaitIgnoresCarrierRelease => "wait_ignores_carrier_release",
            Self::WaitForgetsDiscard => "wait_forgets_discard",
            Self::WaitKeepsOriginTurn => "wait_keeps_origin_turn",
            Self::WaitRepeatsUnread => "wait_repeats_unread",
            Self::WaitRepeatsIds => "wait_repeats_ids",
            Self::RestoreRemembersWaits => "restore_remembers_waits",
            Self::RouteSkipsClaim => "route_skips_claim",
            Self::ScopeJobsOverlap => "scope_jobs_overlap",
            Self::RestoreSharesClaim => "restore_shares_claim",
            Self::ScopeJobsStopInline => "scope_jobs_stop_inline",
            Self::ScopeLocksKept => "scope_locks_kept",
        }
    }

    pub fn names() -> String {
        Self::ALL.map(Self::name).join("|")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    Close,
    Cancel,
}

struct Shape {
    children: u64,
    pooled: u64,
    resident_max: usize,
    stop_after: Duration,
    stop: Stop,
    script: Script,
}

impl Shape {
    fn of(seed: u64, mutation: AgentsMutation) -> Self {
        let mut rng = Rng::derive(seed, "agents-shape");
        let children = rng.range(CHILDREN_MIN, CHILDREN_MAX);
        let resident_max = rng.range(0, RESIDENT_MAX);
        let stop = if rng.chance(CANCEL_PPM) {
            Stop::Cancel
        } else {
            Stop::Close
        };
        let script = Script {
            steps_max: rng.range(1, TURN_STEPS_MAX),
            step_latency_ms_min: STEP_LATENCY_MS_MIN,
            step_latency_ms_max: rng.range(STEP_LATENCY_MS_MIN, STEP_LATENCY_MS_MAX),
            restore_latency_ms_max: rng.range(0, RESTORE_LATENCY_MS_MAX),
            shutdown_latency_ms_max: rng.range(0, SHUTDOWN_LATENCY_MS_MAX),
            mutation,
        };
        let turn_ms_max = script.steps_max * script.step_latency_ms_max;
        let stop_after = Duration::from_millis(rng.range(0, turn_ms_max));
        let pooled = rng.range(1, POOLED_MAX);
        Self {
            children,
            pooled,
            resident_max: usize::try_from(resident_max).expect("a resident bound fits in usize"),
            stop_after,
            stop,
            script,
        }
    }

    fn describe(&self) -> String {
        format!(
            "scenario=agents children={} pooled={} resident<={} stop={:?} after_ms={} steps<={} latency_ms<={} restore_ms<={} shutdown_ms<={}",
            self.children,
            self.pooled,
            self.resident_max,
            self.stop,
            self.stop_after.as_millis(),
            self.script.steps_max,
            self.script.step_latency_ms_max,
            self.script.restore_latency_ms_max,
            self.script.shutdown_latency_ms_max
        )
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AgentStats {
    pub seeds: u64,
    pub closes: u64,
    pub cancels: u64,
    pub children: u64,
    pub spawned: u64,
    pub restored: u64,
    pub parked: u64,
    pub steps: u64,
    pub turns: u64,
    pub stop_ms_max: u64,
}

impl AgentStats {
    pub fn add(&mut self, other: &Self) {
        self.seeds += other.seeds;
        self.closes += other.closes;
        self.cancels += other.cancels;
        self.children += other.children;
        self.spawned += other.spawned;
        self.restored += other.restored;
        self.parked += other.parked;
        self.steps += other.steps;
        self.turns += other.turns;
        self.stop_ms_max = self.stop_ms_max.max(other.stop_ms_max);
    }

    fn of(shape: &Shape, counts: Counts, stop_ms: u64) -> Self {
        Self {
            seeds: 1,
            closes: u64::from(shape.stop == Stop::Close),
            cancels: u64::from(shape.stop == Stop::Cancel),
            children: shape.children,
            spawned: counts.spawned,
            restored: counts.restored,
            parked: counts.parked,
            steps: counts.steps,
            turns: counts.turns,
            stop_ms_max: stop_ms,
        }
    }
}

pub struct AgentOutcome {
    pub digest: u64,
    pub wrong: Option<String>,
    pub stats: AgentStats,
    pub shape: String,
}

struct Run {
    registry: Arc<Registry>,
    control: SubagentControl,
    root: AgentHandle,
    updates: UnboundedReceiver<ScopedAgentUpdate>,
    plane: Option<Arc<dyn Coordination>>,
}

fn build(ledger: &Arc<Ledger>, shape: &Shape) -> Run {
    let (registry, control, updates) = nanocode_subagents::channel(DEFAULT_MAX_SUBAGENTS);
    let plane = crate::boardagents::plane();
    control.set_coordination(plane.clone());
    control.set_hibernation(true);
    control.set_max_resident(shape.resident_max);
    registry.set_store(Arc::new(ScriptedStore::new(Arc::clone(ledger))));
    let root = AgentHandle::new(
        ROOT_SESSION,
        simulated_model(),
        Arc::new(ScriptedFactory::new(Arc::clone(ledger))),
    );
    Run {
        registry,
        control,
        root,
        updates,
        plane,
    }
}

fn tasks(children: u64) -> Vec<AgentTask> {
    (0..children)
        .map(|child| AgentTask {
            role: "worker".to_owned(),
            task: format!("task {child}"),
            output_schema: Value::Object(serde_json::Map::new()),
        })
        .collect()
}

async fn stop(run: &Run, shape: &Shape, clock: &Clock) -> Result<u64, String> {
    let began_ns = clock.now_ns();
    match shape.stop {
        Stop::Close => run
            .control
            .close_all(ROOT_SESSION)
            .await
            .map_err(|error| format!("close_all failed: {error}"))?,
        Stop::Cancel => run.control.cancel_all(ROOT_SESSION).await,
    }
    let took = Duration::from_nanos(clock.now_ns() - began_ns);
    let took_ms = u64::try_from(took.as_millis()).expect("a virtual stop fits in u64 milliseconds");
    if took >= RUNTIME_STOP_TIMEOUT {
        return Err(format!(
            "{:?} took {took_ms} ms of virtual time, not under the runtime's stop deadline of {} ms",
            shape.stop,
            RUNTIME_STOP_TIMEOUT.as_millis()
        ));
    }
    Ok(took_ms)
}

const fn settled(stop: Stop, status: &AgentStatus) -> bool {
    match stop {
        Stop::Close => matches!(status, AgentStatus::Closed),
        Stop::Cancel => matches!(
            status,
            AgentStatus::Interrupted | AgentStatus::Completed { .. } | AgentStatus::Failed { .. }
        ),
    }
}

async fn judge(
    run: &Run,
    shape: &Shape,
    ledger: &Ledger,
    retired: &[AgentId],
    hosted: u64,
) -> Result<(), String> {
    let turns = ledger.running_turns();
    if turns > 0 {
        return Err(format!(
            "{turns} children still hold a turn after {:?}",
            shape.stop
        ));
    }
    let open = ledger.open_agents();
    if shape.stop == Stop::Close && open > 0 {
        return Err(format!(
            "{open} children still hold a resident harness after Close"
        ));
    }
    let listed = run
        .registry
        .directory(ROOT_SESSION, true, false)
        .await
        .map_err(|error| {
            format!(
                "the directory did not answer after {:?}: {error}",
                shape.stop
            )
        })?;
    let count = u64::try_from(listed.len()).expect("a child count fits in u64");
    if count != shape.children + shape.pooled + hosted {
        return Err(format!(
            "the directory lists {count} of {} children, {} pooled and {hosted} hosted agents",
            shape.children, shape.pooled
        ));
    }
    for entry in listed {
        let closed = retired.contains(&entry.agent_id);
        if closed && entry.status != AgentStatus::Closed {
            return Err(format!(
                "the pool retired agent {} but it is {:?} after {:?}",
                entry.agent_id, entry.status, shape.stop
            ));
        }
        if !closed && !settled(shape.stop, &entry.status) {
            return Err(format!(
                "agent {} is {:?} after {:?}",
                entry.agent_id, entry.status, shape.stop
            ));
        }
    }
    if let Some(plane) = &run.plane {
        board_agrees(run, plane, true).await?;
        if let Some(error) = crate::boardagents::audit().await {
            return Err(error);
        }
    }
    Ok(())
}

async fn board_agrees(
    run: &Run,
    plane: &Arc<dyn Coordination>,
    settled: bool,
) -> Result<(), String> {
    let listed = run
        .registry
        .directory(ROOT_SESSION, true, false)
        .await
        .map_err(|error| format!("list_agents from the board failed: {error}"))?;
    let ids = listed
        .iter()
        .map(|entry| entry.agent_id)
        .collect::<Vec<_>>();
    let (waited, ..) = run
        .registry
        .wait(ROOT_SESSION, "board-agrees", &ids, Duration::ZERO)
        .await
        .map_err(|error| format!("wait_agent from the board failed: {error}"))?;
    run.registry.set_coordination(None);
    let registry = run.registry.directory(ROOT_SESSION, true, false).await;
    run.registry.set_coordination(Some(Arc::clone(plane)));
    let registry = registry.map_err(|error| format!("the registry did not list: {error}"))?;
    let board = plane
        .agents(ROOT_SESSION)
        .await
        .map_err(|error| format!("the board did not read: {error}"))?;
    let truth = |id| {
        registry
            .iter()
            .find(|entry| entry.agent_id == id)
            .map(|entry| &entry.status)
    };
    let finished = |status: &AgentStatus| {
        matches!(
            status,
            AgentStatus::Completed { .. } | AgentStatus::Failed { .. }
        )
    };
    if settled && listed.len() != registry.len() {
        return Err(format!(
            "list_agents from the board lists {} of the registry's {} children",
            listed.len(),
            registry.len()
        ));
    }
    let answers = listed
        .iter()
        .map(|entry| ("list_agents", entry.agent_id, &entry.status))
        .chain(
            waited
                .iter()
                .map(|summary| ("wait_agent", summary.agent_id, &summary.status)),
        );
    for entry in &registry {
        let read = board.get(&entry.agent_id);
        let agrees = match read {
            _ if !finished(&entry.status) => true,
            Some(Some(status)) => *status == entry.status,
            _ => !settled,
        };
        if !agrees {
            return Err(format!(
                "the board reads agent {} as {read:?}, the registry {:?}",
                entry.agent_id, entry.status
            ));
        }
    }
    for (tool, id, status) in answers {
        let expected = truth(id);
        let lagging = !settled && !finished(status);
        if expected != Some(status) && !lagging {
            return Err(format!(
                "{tool} from the board says agent {id} is {status:?}, the registry {expected:?}"
            ));
        }
    }
    Ok(())
}

fn pool_policy() -> Policy {
    Policy {
        roles: vec![Role {
            name: "worker".to_owned(),
            labels: [(POOL_KIND.to_owned(), POOL_WORK.to_owned())]
                .into_iter()
                .collect(),
            cost_nano_usd: 1,
            placement: "sim".to_owned(),
        }],
        member_label: POOL_MEMBER.to_owned(),
        project_label: POOL_PROJECT.to_owned(),
        intent_prefix: "dispatch/".to_owned(),
        budgets: Budgets {
            agents_per_member_max: POOLED_MAX,
            agents_per_project_max: POOLED_MAX,
            nano_usd_max: POOLED_MAX,
        },
        idle_retire_ms: POOL_IDLE_RETIRE_MS,
        start_timeout_ms: POOL_LEASE_MS,
        start_attempts_max: 1,
    }
}

fn shown(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

fn board_ms(clock: &Clock) -> u64 {
    POOL_CHARTER_MS + clock.now_ns() / 1_000_000
}

async fn pool_board(
    host: &Arc<dyn LogHost>,
    shape: &Shape,
    clock: &Clock,
) -> Result<Dispatcher, String> {
    let open = |actor: &str| Board::open(Arc::clone(host), POOL_SCOPE, identity(actor));
    open(POOL_OWNER)
        .await
        .map_err(shown)?
        .charter(POOL_CHARTER_MS)
        .await
        .map_err(shown)?;
    let mut poster = open(POOL_POSTER).await.map_err(shown)?;
    for index in 0..shape.pooled {
        let labels = [
            (POOL_KIND, POOL_WORK),
            (POOL_MEMBER, "member"),
            (POOL_PROJECT, "project"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect();
        let task = NewTask {
            id: TaskId(format!("pooled-{index}")),
            parent: None,
            spec: format!("pooled task {index}"),
            labels,
            needs: Vec::new(),
            terms: Default::default(),
        };
        poster
            .submit(board_ms(clock), Op::Post { task })
            .await
            .map_err(|error| format!("the poster could not post pooled-{index}: {error:?}"))?;
    }
    let board = open(POOL_OWNER).await.map_err(shown)?;
    Ok(Dispatcher::new(board, pool_policy(), BRIEF))
}

async fn claim_work(
    host: &Arc<dyn LogHost>,
    worker: &str,
    task: &TaskId,
    at_ms: u64,
) -> Result<(), String> {
    let mut board = Board::open(Arc::clone(host), POOL_SCOPE, identity(worker))
        .await
        .map_err(shown)?;
    if !matches!(
        board.fold().task(task).map(|task| &task.phase),
        Some(Phase::Open)
    ) {
        return Ok(());
    }
    let claim = Op::Claim {
        task: task.clone(),
        lease_ms: POOL_LEASE_MS,
        fence: None,
        cap: None,
    };
    board
        .submit(at_ms, claim)
        .await
        .map_err(|error| format!("{worker} could not claim {task}: {error:?}"))?;
    Ok(())
}

async fn dispatch(
    dispatcher: &mut Dispatcher,
    pool: &SubagentPool,
    at_ms: u64,
) -> Result<(), String> {
    let pass = dispatcher
        .pass(pool, at_ms)
        .await
        .map_err(|error| format!("a dispatcher pass over the pool failed: {error:?}"))?;
    match pass.failures.first() {
        Some(failure) => Err(format!(
            "the dispatcher could not drive the pool: {failure:?}"
        )),
        None => Ok(()),
    }
}

async fn placed(
    host: &Arc<dyn LogHost>,
    dispatcher: &mut Dispatcher,
    pool: &SubagentPool,
    shape: &Shape,
    clock: &Clock,
) -> Result<(), String> {
    let policy = pool_policy();
    for _ in 0..POOL_PASSES_MAX {
        dispatch(dispatcher, pool, board_ms(clock)).await?;
        let intents = nanocode_dispatch::intents(dispatcher.board().fold(), &policy);
        let mut live = 0;
        for agent in pool.agents().await {
            let Some(intent) = intents
                .iter()
                .find(|intent| agent.intent.as_ref() == Some(&intent.id))
            else {
                continue;
            };
            let settled = IntentState::Live {
                agent: agent.id.clone(),
            };
            live += u64::from(intent.state == settled);
            let worker = dispatcher.worker_of(&agent.id);
            claim_work(host, &worker, &intent.task, board_ms(clock)).await?;
        }
        if live == shape.pooled {
            break;
        }
    }
    let intents = nanocode_dispatch::intents(dispatcher.board().fold(), &policy);
    let listed = pool.agents().await;
    if u64::try_from(intents.len()).expect("an intent count fits in u64") != shape.pooled {
        return Err(format!(
            "the dispatcher recorded {} intents for {} pooled tasks",
            intents.len(),
            shape.pooled
        ));
    }
    for intent in &intents {
        let IntentState::Live { agent } = &intent.state else {
            return Err(format!(
                "intent {} for {} is {:?} after {POOL_PASSES_MAX} passes",
                intent.id, intent.task, intent.state
            ));
        };
        let Some(listed) = listed.iter().find(|listed| listed.id == *agent) else {
            return Err(format!(
                "the board settled {} on {agent} but the pool does not list it",
                intent.id
            ));
        };
        if listed.intent.as_ref() != Some(&intent.id) {
            return Err(format!(
                "the pool lists {agent} under {:?}, the board settled it for {}",
                listed.intent, intent.id
            ));
        }
    }
    if listed.len() != intents.len() {
        return Err(format!(
            "the pool lists {} agents for {} intents",
            listed.len(),
            intents.len()
        ));
    }
    Ok(())
}

async fn pooled(run: &Run, shape: &Shape, clock: &Arc<Clock>) -> Result<Vec<AgentId>, String> {
    let now = Arc::clone(clock);
    let pool = SubagentPool::new(
        run.root.clone(),
        Arc::clone(&run.registry),
        ROOT_SESSION,
        Box::new(move || board_ms(&now)),
    );
    let host: Arc<dyn LogHost> = Arc::new(MemoryLog::new());
    let mut dispatcher = pool_board(&host, shape, clock).await?;
    placed(&host, &mut dispatcher, &pool, shape, clock).await?;
    let Some(first) = pool.agents().await.first().map(|agent| agent.id.clone()) else {
        return Err("the dispatcher placed no pooled agent".to_owned());
    };
    let Some(idled) = pool.agent_id(&first) else {
        return Err(format!("the pool forgot {first} before retiring it"));
    };
    run.registry
        .interrupt(ROOT_SESSION, idled)
        .await
        .map_err(|error| format!("interrupting {first} failed: {error}"))?;
    let idle = pool
        .agents()
        .await
        .into_iter()
        .filter_map(|agent| Some((pool.agent_id(&agent.id)?, agent.id)))
        .collect::<Vec<_>>();
    dispatch(
        &mut dispatcher,
        &pool,
        board_ms(clock) + POOL_IDLE_RETIRE_MS,
    )
    .await?;
    let kept = pool.agents().await;
    let gone = idle
        .iter()
        .filter(|(_, name)| !kept.iter().any(|agent| agent.id == *name))
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    if !gone.contains(&idled) {
        return Err(format!(
            "the dispatcher left {first} (agent {idled}) idle past idle_retire_ms"
        ));
    }
    let directory = run
        .registry
        .directory(ROOT_SESSION, true, false)
        .await
        .map_err(|error| format!("the directory did not answer after a retire: {error}"))?;
    for id in &gone {
        let status = directory
            .iter()
            .find(|entry| entry.agent_id == *id)
            .map(|entry| &entry.status);
        if status != Some(&AgentStatus::Closed) {
            return Err(format!(
                "the dispatcher retired agent {id} but the runtime has it {status:?}"
            ));
        }
    }
    Ok(gone)
}

/// Posts pooled work to the dispatcher the board hosts for the root, the
/// production path, and waits until it placed an agent on each task.
async fn hosted(shape: &Shape) -> Result<u64, String> {
    let Some(plane) = crate::boardagents::hosting_plane() else {
        return Ok(0);
    };
    if !plane.hosting().iter().any(|root| root == ROOT_SESSION) {
        return Err("the board hosts no dispatcher for the root session".to_owned());
    }
    for index in 0..shape.pooled {
        let labels = [
            (DISPATCH_LABEL, DISPATCH_ROLE),
            (POOL_MEMBER, "member"),
            (POOL_PROJECT, "project"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect();
        let task = NewTask {
            id: TaskId(format!("hosted-{index}")),
            parent: None,
            spec: format!("hosted task {index}"),
            labels,
            needs: Vec::new(),
            terms: Default::default(),
        };
        plane.post_dispatch(ROOT_SESSION, task).await?;
    }
    let mut waiting = "no pass yet".to_owned();
    for _ in 0..HOSTED_PASSES_MAX {
        nanocode_platform::sleep(HOSTED_POLL).await;
        let Some(view) = plane.hosted(ROOT_SESSION).await? else {
            return Err("the hosted dispatcher stopped before its work was placed".to_owned());
        };
        match hosted_placed(&view, shape.pooled)? {
            None => return Ok(shape.pooled),
            Some(reason) => waiting = reason,
        }
    }
    Err(format!(
        "the hosted dispatcher did not place its work after {HOSTED_PASSES_MAX} polls: {waiting}"
    ))
}

/// `Err` when the hosted pool and its board disagree, `Ok(Some(_))` while
/// placement is still under way, `Ok(None)` once every task has its agent.
fn hosted_placed(view: &HostedView, want: u64) -> Result<Option<String>, String> {
    for agent in &view.agents {
        let Some(intent) = &agent.intent else {
            return Err(format!(
                "the hosted pool lists {} without the intent it was placed for",
                agent.id
            ));
        };
        if !view.intents.iter().any(|known| known.id == *intent) {
            return Err(format!(
                "the hosted pool lists {} under {intent}, which {} never recorded",
                agent.id, view.scope
            ));
        }
    }
    if u64::try_from(view.intents.len()).expect("an intent count fits in u64") != want {
        return Ok(Some(format!(
            "{} intents for {want} hosted tasks",
            view.intents.len()
        )));
    }
    for intent in &view.intents {
        let phase = view.phases.get(&intent.task.0);
        // A worker that claimed its task and then ended it was placed. The
        // scripted harness never calls submit_result, so each worker's turn
        // ends Failed once it runs out of steps, and a slow placement (a
        // lagging board costs the dispatcher a pass) lets the first workers
        // end before the last one claims.
        if matches!(
            phase,
            Some(
                Phase::Done { .. }
                    | Phase::Failed {
                        worker: Some(_),
                        ..
                    }
            )
        ) {
            continue;
        }
        let IntentState::Live { agent } = &intent.state else {
            return Ok(Some(format!("intent {} is {:?}", intent.id, intent.state)));
        };
        let Some(listed) = view.agents.iter().find(|listed| listed.id == *agent) else {
            return Err(format!(
                "{} settled {} on {agent} but the hosted pool does not list it",
                view.scope, intent.id
            ));
        };
        if listed.intent.as_ref() != Some(&intent.id) {
            return Err(format!(
                "the hosted pool lists {agent} under {:?}, {} settled it for {}",
                listed.intent, view.scope, intent.id
            ));
        }
        match phase {
            Some(Phase::Claimed { worker, .. }) if worker.ends_with(&format!("+{agent}")) => {}
            Some(Phase::Claimed { worker, .. }) => {
                return Err(format!(
                    "{} is claimed by {worker}, not by {agent} that the dispatcher placed on it",
                    intent.task
                ));
            }
            other => return Ok(Some(format!("{} is {other:?}", intent.task))),
        }
    }
    Ok(None)
}

async fn steer_running(run: &Run) -> Result<(), String> {
    let listed = run
        .registry
        .directory(ROOT_SESSION, true, false)
        .await
        .map_err(|error| format!("the directory did not answer before steering: {error}"))?;
    for entry in listed {
        if entry.status == AgentStatus::Running {
            crate::boardagents::steer(ROOT_SESSION, entry.agent_id).await;
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum WaitCarrier {
    Direct,
    Transferred,
    Discarded,
}

async fn waits_once(ledger: &Arc<Ledger>, scenario: WaitCarrier) -> Result<(), String> {
    let session = match scenario {
        WaitCarrier::Direct => "wait-direct",
        WaitCarrier::Transferred => "wait-transfer",
        WaitCarrier::Discarded => "wait-discard",
    };
    let (registry, control, updates) = nanocode_subagents::channel(DEFAULT_MAX_SUBAGENTS);
    let root = AgentHandle::new(
        session,
        simulated_model(),
        Arc::new(ScriptedFactory::new(Arc::clone(ledger))),
    );
    let start = || nanocode_subagents::start_agent(&root, &registry, session, tasks(1).remove(0));
    let first = start().await.map_err(shown)?.agent_id;
    let poll = Duration::from_millis(STEP_LATENCY_MS_MIN);
    loop {
        let listed = registry
            .directory(session, true, false)
            .await
            .map_err(shown)?;
        if listed
            .iter()
            .any(|child| child.agent_id == first && !child.status.is_active())
        {
            break;
        }
        nanocode_platform::sleep(poll).await;
    }
    let second = start().await.map_err(shown)?.agent_id;
    let (parent, events, publisher) = ledger.agent_with_publisher(session, simulated_model());
    registry.await_restored_root(&parent, session).await;
    publisher
        .publish_next(AgentEventKind::RunStarted, raw_run("cell", None)?)
        .map_err(shown)?;
    registry.set_parent_resolver(Arc::new(move |root| {
        (root == session).then(|| parent.clone())
    }));
    let ids = [first, second];
    let bound = poll / 4;
    nanocode_platform::sleep(bound).await;
    let mut discard = None;
    let carrier = if !matches!(scenario, WaitCarrier::Direct) {
        let tools =
            nanocode_subagents::install_tools(WaitTools::default(), root, Arc::clone(&registry))
                .map_err(shown)?;
        let tools = Arc::new(tools);
        let turn = Arc::new(std::sync::atomic::AtomicU64::new(1));
        let runtime = CodeModeRuntime::new_with_turn(Arc::clone(&turn));
        let mut source = format!(
            "// @exec: {{\"yield_time_ms\": {}}}\nconsole.log(await tools.wait_agent({}));",
            bound.as_millis() / 4,
            serde_json::json!({"agent_ids": ids, "timeout_ms": bound.as_millis()})
        );
        if matches!(scenario, WaitCarrier::Discarded) {
            source.push_str("await tools.hold({});");
        }
        let context = OwnedToolContext::new("sim", session, "first", Arc::default(), 1000)
            .with_turn_id(Some(Arc::from("cell")));
        let yielded = runtime.execute(&source, tools.clone(), context).await;
        if !yielded.cell.is_some_and(|cell| cell.running) {
            return Err(format!(
                "exec did not yield before wait_agent returned: {:?}",
                yielded.output
            ));
        }
        if matches!(scenario, WaitCarrier::Discarded) {
            tools.gate.fire();
            tools.holding.wait().await;
        }
        publisher
            .publish_next(AgentEventKind::RunFailed, raw_run("cell", Some("failed"))?)
            .map_err(shown)?;
        runtime
            .control()
            .detach_turn(1, &mut |_| Box::new(QuietCellObserver))
            .await;
        turn.store(2, std::sync::atomic::Ordering::Release);
        runtime.control().begin_turn();
        publisher
            .publish_next(AgentEventKind::RunStarted, raw_run("observer", None)?)
            .map_err(shown)?;
        tools.gate.fire();
        if matches!(scenario, WaitCarrier::Discarded) {
            discard = Some(runtime);
        } else {
            let context = ToolContext::new("sim", session, "observe", &[], 1000)
                .with_turn_id(Some("observer"));
            let observed = runtime
                .wait(r#"{"cell_id":1,"yield_time_ms":50}"#, context)
                .await;
            if !observed.success || !observed.cell.is_some_and(|cell| !cell.running) {
                return Err(format!(
                    "the next turn did not collect the yielded cell: {:?}",
                    observed.output
                ));
            }
            if !observed.nested_calls.iter().any(|call| {
                call.success
                    && call.name == "wait_agent"
                    && call.structured_result["timed_out"] == false
                    && matches!(
                        call.structured_result["agents"][0]["status"]["state"].as_str(),
                        Some("completed" | "failed")
                    )
            }) {
                return Err(format!(
                    "the yielded cell did not report a terminal child: {:?}",
                    observed.output
                ));
            }
        }
        "observer"
    } else {
        let (agents, delivered, timed_out, receipt) = registry
            .wait(session, "first", &ids, bound)
            .await
            .map_err(shown)?;
        if timed_out || delivered.contains(&first) || !agents[1].status.is_active() {
            return Err(
                "the first cell wait did not deliver the completed child while the other ran"
                    .to_owned(),
            );
        }
        let _receipt = receipt.ok_or("the first cell wait has no receipt")?;
        "cell"
    };
    let started = nanocode_platform::now();
    let (agents, delivered, timed_out, receipt) = registry
        .wait(session, "repeat", &ids, bound)
        .await
        .map_err(shown)?;
    if !timed_out
        || nanocode_platform::elapsed(started) < bound
        || !delivered.contains(&first)
        || !agents[1].status.is_active()
        || receipt.is_some()
    {
        return Err(format!(
            "{scenario:?}: a repeated wait returned an unread child instead of timing out"
        ));
    }
    if matches!(scenario, WaitCarrier::Transferred) {
        for (kind, event) in [
            (
                AgentEventKind::ToolResult,
                serde_json::json!({"call_id": "observe", "turn_id": carrier}),
            ),
            (
                AgentEventKind::ModelCallStarted,
                serde_json::json!({"call_index": 1, "turn_id": carrier}),
            ),
        ] {
            publisher
                .publish_next(
                    kind,
                    Arc::from(serde_json::value::to_raw_value(&event).map_err(shown)?),
                )
                .map_err(shown)?;
        }
    }
    let started = nanocode_platform::now();
    let wait = Box::pin(registry.wait(session, "after-cell", &ids, bound));
    let timer = Box::pin(nanocode_platform::sleep(bound / 4));
    let wait = match nanocode_sync::future::select(wait, timer).await {
        nanocode_sync::future::Either::Left(_) => {
            return Err("wait returned before the carrier ended".into());
        }
        nanocode_sync::future::Either::Right((_, wait)) => wait,
    };
    if let Some(runtime) = discard {
        runtime.control().terminate_all(None).await;
    } else {
        let status = match scenario {
            WaitCarrier::Direct => "failed",
            _ => "cancelled",
        };
        publisher
            .publish_next(AgentEventKind::RunFailed, raw_run(carrier, Some(status))?)
            .map_err(shown)?;
    }
    let (agents, delivered, timed_out, _) = wait.await.map_err(shown)?;
    if timed_out
        || delivered.contains(&first)
        || !agents[1].status.is_active()
        || nanocode_platform::elapsed(started) >= bound
    {
        return Err(format!(
            "{scenario:?}: a blocked wait did not return the unread result when its carrier ended"
        ));
    }
    control.close_all(session).await.map_err(shown)?;
    drop((registry, updates, events));
    Ok(())
}

struct QuietCellObserver;

impl CodeModeObserver for QuietCellObserver {
    fn update(&mut self, _: nanocode_code_mode::code_mode::CodeModeUpdate<'_>) {}
}

#[derive(Default)]
struct WaitTools {
    inner: Vec<Arc<dyn Tool>>,
    gate: nanocode_sync::Signal,
    holding: nanocode_sync::Signal,
}

impl ToolCollection for WaitTools {
    type Error = std::convert::Infallible;

    fn add_shared_tool(mut self, tool: Arc<dyn Tool>) -> Result<Self, Self::Error> {
        self.inner.push(tool);
        Ok(self)
    }
}

impl CodeModeToolset for WaitTools {
    fn nested_tool_metadata(&self) -> Vec<Value> {
        vec![
            serde_json::json!({"kind": "function", "name": "wait_agent", "tool_name": "wait_agent"}),
            serde_json::json!({"kind": "function", "name": "hold", "tool_name": "hold"}),
        ]
    }

    fn execute_nested<'a>(
        &'a self,
        name: &'a str,
        input: Value,
        context: ToolContext<'a>,
    ) -> HostFuture<'a, ToolOutput> {
        Box::pin(async move {
            if name == "hold" {
                self.holding.fire();
                return std::future::pending().await;
            }
            self.gate.wait().await;
            let tool = self
                .inner
                .iter()
                .find(|tool| tool.definition().name() == name)
                .unwrap();
            tool.execute(
                ToolInput::Function(serde_json::value::to_raw_value(&input).unwrap()),
                context,
            )
            .await
            .unwrap()
        })
    }
}

fn raw_run(turn: &str, status: Option<&str>) -> Result<Arc<serde_json::value::RawValue>, String> {
    serde_json::value::to_raw_value(&serde_json::json!({"turn_id": turn, "status": status}))
        .map(Arc::from)
        .map_err(shown)
}

const REDELIVER_SESSION: &str = "redeliver";

async fn handed_out(
    registry: &Arc<Registry>,
    call: &str,
    ids: &[AgentId],
    bound: Duration,
    handed: &mut std::collections::BTreeMap<AgentId, u64>,
) -> Result<bool, String> {
    let asked = ids.iter().chain(ids).copied().collect::<Vec<_>>();
    let (summaries, delivered, ..) = registry
        .wait(REDELIVER_SESSION, call, &asked, bound)
        .await
        .map_err(|error| format!("wait_agent {call} failed: {error}"))?;
    let mut listed = std::collections::BTreeSet::new();
    let mut fresh = false;
    for summary in summaries {
        if !listed.insert(summary.agent_id) {
            return Err(format!(
                "wait_agent {call} asked for each child twice and listed agent {} twice",
                summary.agent_id
            ));
        }
        if !summary.status.is_active() && !delivered.contains(&summary.agent_id) {
            *handed.entry(summary.agent_id).or_default() += 1;
            fresh = true;
        }
    }
    Ok(fresh)
}

async fn redelivered(shape: &Shape, ledger: &Arc<Ledger>) -> Result<(), String> {
    let store: Arc<dyn nanocode_subagents::SubagentStore> =
        Arc::new(ScriptedStore::new(Arc::clone(ledger)));
    let open = || {
        let (registry, control, updates) = nanocode_subagents::channel(DEFAULT_MAX_SUBAGENTS);
        control.set_hibernation(true);
        control.set_max_resident(shape.resident_max);
        registry.set_store(Arc::clone(&store));
        (registry, control, updates)
    };
    let (registry, control, updates) = open();
    let root = AgentHandle::new(
        REDELIVER_SESSION,
        simulated_model(),
        Arc::new(ScriptedFactory::new(Arc::clone(ledger))),
    );
    let ids = nanocode_subagents::start_agents(
        &root,
        &registry,
        REDELIVER_SESSION,
        tasks(shape.children),
    )
    .await
    .map_err(|error| format!("start_agents for the restart check failed: {error}"))?
    .into_iter()
    .map(|report| report.agent_id)
    .collect::<Vec<_>>();
    let turn = Duration::from_millis(shape.script.steps_max * shape.script.step_latency_ms_max);
    let mut before = std::collections::BTreeMap::new();
    handed_out(&registry, "before", &ids, turn, &mut before).await?;
    if let Some((id, times)) = before.iter().find(|(_, times)| **times > 1) {
        return Err(format!(
            "one wait_agent handed agent {id}'s result out {times} times"
        ));
    }
    let poll = Duration::from_millis(shape.script.step_latency_ms_min);
    loop {
        let listed = registry
            .directory(REDELIVER_SESSION, true, false)
            .await
            .map_err(|error| format!("the directory did not answer before the restart: {error}"))?;
        if listed.iter().all(|entry| !entry.status.is_active()) {
            break;
        }
        nanocode_platform::sleep(poll).await;
    }
    control
        .persist(REDELIVER_SESSION)
        .await
        .map_err(|error| format!("the journal did not persist before the restart: {error}"))?;
    ledger.trace("redeliver restart");
    drop((registry, control, updates, root));
    let (registry, control, updates) = open();
    let report = registry
        .restore(REDELIVER_SESSION)
        .await
        .map_err(|error| format!("the restart did not restore its children: {error}"))?;
    if report.restored != ids.len() {
        return Err(format!(
            "the restart restored {} of {} children",
            report.restored,
            ids.len()
        ));
    }
    let mut after = std::collections::BTreeMap::new();
    for call in ["after", "after-again", "after-idle"] {
        handed_out(&registry, call, &ids, turn, &mut after).await?;
    }
    for id in &ids {
        let times = after.get(id).copied().unwrap_or(0);
        if times != 1 {
            return Err(format!(
                "after a restart the root's waits handed agent {id}'s result out {times} times, not once (before the restart: {})",
                before.get(id).copied().unwrap_or(0)
            ));
        }
    }
    control
        .close_all(REDELIVER_SESSION)
        .await
        .map_err(|error| format!("close_all after the restart check failed: {error}"))?;
    drop((registry, updates));
    Ok(())
}

const ROUTE_SESSION: &str = "route-once";
const ROUTE_CHILDREN: u64 = 1;

async fn routed_once(shape: &Shape, ledger: &Arc<Ledger>) -> Result<(), String> {
    let (registry, control, updates) = nanocode_subagents::channel(DEFAULT_MAX_SUBAGENTS);
    let root = AgentHandle::new(
        ROUTE_SESSION,
        simulated_model(),
        Arc::new(ScriptedFactory::new(Arc::clone(ledger))),
    );
    let ids =
        nanocode_subagents::start_agents(&root, &registry, ROUTE_SESSION, tasks(ROUTE_CHILDREN))
            .await
            .map_err(|error| format!("start_agents for the route check failed: {error}"))?
            .into_iter()
            .map(|report| report.agent_id)
            .collect::<Vec<_>>();
    let poll = Duration::from_millis(shape.script.step_latency_ms_min);
    loop {
        let listed = registry
            .directory(ROUTE_SESSION, true, false)
            .await
            .map_err(|error| {
                format!("the directory did not answer before the route check: {error}")
            })?;
        if listed.iter().all(|entry| !entry.status.is_active()) {
            break;
        }
        nanocode_platform::sleep(poll).await;
    }
    let (parent, events) = ledger.agent(ROUTE_SESSION, simulated_model());
    let (first, second) = nanocode_sync::join!(
        registry.deliver_completions(&parent, ROUTE_SESSION),
        registry.deliver_completions(&parent, ROUTE_SESSION),
    );
    let mut routed = std::collections::BTreeMap::<AgentId, u64>::new();
    for (id, result) in first.into_iter().chain(second) {
        if matches!(result, Ok(true)) {
            *routed.entry(id).or_default() += 1;
        }
    }
    if let Some((id, times)) = routed.iter().find(|(_, times)| **times > 1) {
        return Err(format!(
            "two dispatchers racing over one completion routed agent {id}'s completion to the parent {times} times"
        ));
    }
    let turn = Duration::from_millis(shape.script.steps_max * shape.script.step_latency_ms_max);
    nanocode_platform::sleep(turn).await;
    if let Some((request, times)) = ledger.repeated_request(ROUTE_SESSION) {
        return Err(format!(
            "two dispatchers racing over one completion began the parent's turn {request} {times} times"
        ));
    }
    if let Some(id) = ids.iter().find(|id| !routed.contains_key(id)) {
        return Err(format!(
            "neither of two dispatchers routed agent {id}'s completion to the parent"
        ));
    }
    control
        .close_all(ROUTE_SESSION)
        .await
        .map_err(|error| format!("close_all after the route check failed: {error}"))?;
    drop((registry, updates, parent, events));
    Ok(())
}

const ORDER_SESSION: &str = "scope-order";

async fn scoped_in_order(ledger: &Arc<Ledger>) -> Result<(), String> {
    let (registry, control, updates) = nanocode_subagents::channel(DEFAULT_MAX_SUBAGENTS);
    let root = AgentHandle::new(
        ORDER_SESSION,
        simulated_model(),
        Arc::new(ScriptedFactory::new(Arc::clone(ledger))),
    );
    let ids = nanocode_subagents::start_agents(&root, &registry, ORDER_SESSION, tasks(1))
        .await
        .map_err(|error| format!("start_agents for the scope order check failed: {error}"))?
        .into_iter()
        .map(|report| report.agent_id)
        .collect::<Vec<_>>();
    let [id] = ids[..] else {
        return Err(format!(
            "start_agents started {} agents for one task",
            ids.len()
        ));
    };
    let (closed, interrupted) = nanocode_sync::join!(
        registry.close(ORDER_SESSION, id),
        registry.interrupt(ORDER_SESSION, id),
    );
    closed.map_err(|error| format!("closing agent {id} failed: {error}"))?;
    if let Ok(summaries) = interrupted
        && let Some(summary) = summaries.iter().find(|summary| summary.agent_id == id)
        && summary.status != AgentStatus::Closed
    {
        return Err(format!(
            "an interrupt queued behind the close of agent {id} ran before the close finished: it reads the agent as {:?}",
            summary.status
        ));
    }
    control
        .close_all(ORDER_SESSION)
        .await
        .map_err(|error| format!("close_all after the scope order check failed: {error}"))?;
    drop((registry, updates, root));
    Ok(())
}

const CLAIM_SESSION: &str = "residency-claims";
const CLAIM_SENDS: usize = 4;
const CLAIM_SETTLE: Duration = Duration::from_secs(600);

async fn restored_once(ledger: &Arc<Ledger>) -> Result<(), String> {
    let (registry, control, updates) = nanocode_subagents::channel(DEFAULT_MAX_SUBAGENTS);
    registry.set_max_resident(0);
    let root = AgentHandle::new(
        CLAIM_SESSION,
        simulated_model(),
        Arc::new(ScriptedFactory::new(Arc::clone(ledger))),
    );
    let ids = nanocode_subagents::start_agents(&root, &registry, CLAIM_SESSION, tasks(1))
        .await
        .map_err(|error| format!("start_agents for the residency claim check failed: {error}"))?
        .into_iter()
        .map(|report| report.agent_id)
        .collect::<Vec<_>>();
    let [id] = ids[..] else {
        return Err(format!(
            "start_agents started {} agents for one task",
            ids.len()
        ));
    };
    nanocode_platform::sleep(CLAIM_SETTLE).await;
    let before = ledger.counts().restored;
    let sent = nanocode_sync::future::join_all((0..CLAIM_SENDS).map(|send| {
        registry.send_message(
            CLAIM_SESSION,
            None,
            id,
            MessagePriority::Deferred,
            MessagePurpose::Coordinate,
            None,
            format!("message {send}"),
        )
    }))
    .await;
    let restored = ledger.counts().restored - before;
    if restored > 1 {
        return Err(format!(
            "{CLAIM_SENDS} messages to evicted agent {id} restored it {restored} times: a restore ran while another held its residency claim"
        ));
    }
    if let Some(error) = sent.into_iter().find_map(Result::err) {
        return Err(format!("a message to evicted agent {id} failed: {error}"));
    }
    control
        .close_all(CLAIM_SESSION)
        .await
        .map_err(|error| format!("close_all after the residency claim check failed: {error}"))?;
    drop((registry, updates, root));
    Ok(())
}

const ASIDE_SESSION: &str = "scope-stops";
const ASIDE_QUEUED: Duration = Duration::from_millis(1);

async fn stopped_aside(ledger: &Arc<Ledger>) -> Result<(), String> {
    let (registry, control, updates) = nanocode_subagents::channel(DEFAULT_MAX_SUBAGENTS);
    let root = AgentHandle::new(
        ASIDE_SESSION,
        simulated_model(),
        Arc::new(ScriptedFactory::new(Arc::clone(ledger))),
    );
    let ids = nanocode_subagents::start_agents(&root, &registry, ASIDE_SESSION, tasks(2))
        .await
        .map_err(|error| format!("start_agents for the scope stop check failed: {error}"))?
        .into_iter()
        .map(|report| report.agent_id)
        .collect::<Vec<_>>();
    let [closing, closed] = ids[..] else {
        return Err(format!(
            "start_agents started {} agents for two tasks",
            ids.len()
        ));
    };
    registry
        .close(ASIDE_SESSION, closed)
        .await
        .map_err(|error| format!("closing agent {closed} failed: {error}"))?;
    let ((close, close_at), (interrupt_from, interrupt_at)) = nanocode_sync::join!(
        async {
            let close = registry.close(ASIDE_SESSION, closing).await;
            (close, nanocode_platform::now())
        },
        async {
            nanocode_platform::sleep(ASIDE_QUEUED).await;
            let from = nanocode_platform::now();
            drop(registry.interrupt(ASIDE_SESSION, closed).await);
            (from, nanocode_platform::now())
        },
    );
    close.map_err(|error| format!("closing agent {closing} failed: {error}"))?;
    if close_at > interrupt_from && interrupt_at >= close_at {
        return Err(format!(
            "an interrupt of closed agent {closed} waited {}ms for the harness stop of agent {closing}",
            interrupt_at
                .saturating_duration_since(interrupt_from)
                .as_millis()
        ));
    }
    control
        .close_all(ASIDE_SESSION)
        .await
        .map_err(|error| format!("close_all after the scope stop check failed: {error}"))?;
    control.shutdown().await;
    let kept = registry.scope_locks_kept();
    if kept > 0 {
        return Err(format!(
            "the registry keeps the job state of {kept} scopes after it released them"
        ));
    }
    drop((registry, updates, root));
    Ok(())
}

async fn drive(shape: &Shape, ledger: &Arc<Ledger>, clock: &Arc<Clock>) -> Result<u64, String> {
    let run = build(ledger, shape);
    nanocode_subagents::start_agents(
        &run.root,
        &run.registry,
        ROOT_SESSION,
        tasks(shape.children),
    )
    .await
    .map_err(|error| format!("start_agents failed: {error}"))?;
    ledger.trace("started");
    let retired = pooled(&run, shape, clock).await?;
    ledger.trace("pooled");
    let hosted = hosted(shape).await?;
    if hosted > 0 {
        ledger.trace("hosted");
    }
    if run.plane.is_some() {
        steer_running(&run).await?;
        ledger.trace("steered");
    }
    nanocode_platform::sleep(shape.stop_after).await;
    if let Some(plane) = &run.plane {
        board_agrees(&run, plane, false).await?;
    }
    ledger.trace("stop");
    crate::boardagents::stop_hosting();
    let took_ms = stop(&run, shape, clock).await?;
    ledger.trace("stopped");
    nanocode_platform::sleep(RUNTIME_STOP_TIMEOUT).await;
    judge(&run, shape, ledger, &retired, hosted).await?;
    drop(run.updates);
    waits_once(ledger, WaitCarrier::Direct).await?;
    waits_once(ledger, WaitCarrier::Transferred).await?;
    waits_once(ledger, WaitCarrier::Discarded).await?;
    redelivered(shape, ledger).await?;
    ledger.trace("redelivered");
    routed_once(shape, ledger).await?;
    ledger.trace("routed once");
    scoped_in_order(ledger).await?;
    ledger.trace("scoped in order");
    restored_once(ledger).await?;
    ledger.trace("restored once");
    stopped_aside(ledger).await?;
    ledger.trace("stopped aside");
    Ok(took_ms)
}

pub fn simulate(seed: u64, mutation: AgentsMutation) -> AgentOutcome {
    if mutation == AgentsMutation::BoardFailuresUnread {
        nanocode_board::sim::mutate(nanocode_board::sim::Mutation::SubagentFailuresUnread);
    }
    if mutation == AgentsMutation::PoolRetireKeepsAgent {
        nanocode_board::sim::mutate(nanocode_board::sim::Mutation::PoolRetireKeepsAgent);
    }
    if mutation == AgentsMutation::PoolForgetsIntent {
        nanocode_board::sim::mutate(nanocode_board::sim::Mutation::PoolForgetsIntent);
    }
    if mutation == AgentsMutation::WaitRepeatsIds {
        nanocode_subagents::sim::mutate(nanocode_subagents::sim::Mutation::WaitRepeatsIds);
    }
    if mutation == AgentsMutation::WaitRepeatsUnread {
        nanocode_subagents::sim::mutate(nanocode_subagents::sim::Mutation::WaitRepeatsUnread);
    }
    if mutation == AgentsMutation::WaitIgnoresCarrierRelease {
        nanocode_subagents::sim::mutate(
            nanocode_subagents::sim::Mutation::WaitIgnoresCarrierRelease,
        );
    }
    if mutation == AgentsMutation::WaitKeepsOriginTurn {
        nanocode_subagents::sim::mutate(nanocode_subagents::sim::Mutation::WaitKeepsOriginTurn);
    }
    if mutation == AgentsMutation::WaitForgetsDiscard {
        nanocode_subagents::sim::mutate(nanocode_subagents::sim::Mutation::WaitForgetsDiscard);
    }
    if mutation == AgentsMutation::RestoreRemembersWaits {
        nanocode_subagents::sim::mutate(nanocode_subagents::sim::Mutation::RestoreRemembersWaits);
    }
    if mutation == AgentsMutation::RouteSkipsClaim {
        nanocode_subagents::sim::mutate(nanocode_subagents::sim::Mutation::RouteSkipsClaim);
    }
    if mutation == AgentsMutation::ScopeJobsOverlap {
        nanocode_subagents::sim::mutate(nanocode_subagents::sim::Mutation::ScopeJobsOverlap);
    }
    if mutation == AgentsMutation::RestoreSharesClaim {
        nanocode_subagents::sim::mutate(nanocode_subagents::sim::Mutation::RestoreSharesClaim);
    }
    if mutation == AgentsMutation::ScopeJobsStopInline {
        nanocode_subagents::sim::mutate(nanocode_subagents::sim::Mutation::ScopeJobsStopInline);
    }
    if mutation == AgentsMutation::ScopeLocksKept {
        nanocode_subagents::sim::mutate(nanocode_subagents::sim::Mutation::ScopeLocksKept);
    }
    let shape = Shape::of(seed, mutation);
    let clock = Clock::new();
    crate::host::enter(seed, &clock);
    let ledger = Ledger::new(seed, shape.script, Arc::clone(&clock));
    let result = RefCell::new(None);
    let mut executor = Executor::new(Arc::clone(&clock), Rng::derive(seed, "scheduler"));
    let task: Task<'_> = Box::pin(async {
        let outcome = drive(&shape, &ledger, &clock).await;
        result.replace(Some(outcome));
    });
    let ended = executor.run(vec![task], None);
    let outcome = match (ended, result.into_inner()) {
        (_, Some(outcome)) => outcome,
        (Ended::Stuck, None) => Err("every task waits and no timer is set".to_owned()),
        (ended, None) => Err(format!("the run ended {ended:?} without a verdict")),
    };
    let stop_ms = *outcome.as_ref().unwrap_or(&0);
    nanocode_board::sim::mutate(nanocode_board::sim::Mutation::None);
    nanocode_subagents::sim::mutate(nanocode_subagents::sim::Mutation::None);
    AgentOutcome {
        digest: ledger.digest(),
        wrong: outcome.err(),
        stats: AgentStats::of(&shape, ledger.counts(), stop_ms),
        shape: shape.describe(),
    }
}

pub fn run_seed(seed: u64, mutation: AgentsMutation) -> AgentOutcome {
    std::panic::catch_unwind(|| simulate(seed, mutation)).unwrap_or_else(|panic| {
        let message = panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_owned()))
            .unwrap_or_default();
        AgentOutcome {
            digest: 0,
            wrong: Some(format!("panic: {message}")),
            stats: AgentStats::default(),
            shape: String::new(),
        }
    })
}
