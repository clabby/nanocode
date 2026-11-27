mod agents;
#[cfg(unix)]
mod alloc;
mod antigravitydurable;
mod backendfacts;
mod board;
mod board_brief;
mod board_shards;
mod board_steer;
mod board_sync;
mod boardagents;
#[cfg(unix)]
mod capture;
mod cellcalls;
mod cells;
mod children;
mod claudefacts;
mod codexfacts;
mod controlplane;
mod cost;
mod disk;
mod dispatcher;
#[cfg(target_os = "linux")]
mod dns;
mod drive;
mod durable;
#[cfg(unix)]
mod egress;
mod executor;
mod facts;
mod faults;
mod gcunwind;
mod gitremote;
mod handauto;
mod handrouter;
mod hands;
mod heldawait;
mod holds;
mod host;
mod httpd;
mod httpserver;
mod imports;
mod inboxstress;
mod interpdrop;
mod lease;
mod live;
mod log;
mod longthread;
mod machines;
mod memory;
mod kvlease;
mod scope;
mod threadlegacy;
mod threadjournals;
mod threadscope;
mod threadholds;
mod threadstore;
mod threadturns;
mod threadcompat;
mod threadimport;
mod threadover;
mod threadwakes;
mod migrated;
mod model;
mod mpp;
mod nestedmeta;
mod network;
mod onelog;
mod onelogfacts;
mod partitions;
mod pgpool;
mod plog;
mod refusals;
mod repocache;
mod requestclose;
mod resident;
mod rng;
mod scans;
mod scale;
mod scripted;
mod secrets;
mod server;
mod simulate;
mod slog;
mod append;
mod socket;
mod stall;
mod steercarry;
mod steerorder;
mod store;
mod streams;
mod sync;
mod tasks;
mod tidy;
mod turnlog;
mod turns;
mod wakes;
mod websocket;
mod whole;
mod workload;
mod workspacecrash;
mod workspacefs;
mod workspacehost;
mod workspacesql;
mod workspacestack;
mod world;

use std::{collections::BTreeMap, ops::Range, process::ExitCode};

use crate::{
    agents::{AgentStats, AgentsMutation},
    antigravitydurable::{AntigravityMutation, AntigravityStats},
    backendfacts::BackendFactsMutation,
    board::{BoardMutation, BoardStats},
    board_brief::{BriefMutation as BoardBriefMutation, BriefStats as BoardBriefStats},
    board_shards::{ShardsMutation, ShardsStats},
    board_steer::{SteerMutation as BoardSteerMutation, SteerStats as BoardSteerStats},
    board_sync::{SyncMutation as BoardSyncMutation, SyncStats as BoardSyncStats},
    cellcalls::{CellCallsMutation, CellCallsStats},
    children::ChildMutation,
    controlplane::{ControlplaneMutation, ControlplaneStats},
    cost::{CostMutation, CostStats},
    dispatcher::{DispatcherMutation, DispatcherStats},
    durable::{DurableMutation, DurableStats},
    facts::FactsMutation,
    gcunwind::{GcUnwindMutation, GcUnwindStats},
    gitremote::{GitRemoteMutation, GitRemoteStats},
    handauto::{HandAutoMutation, HandAutoStats},
    handrouter::{HandRouterMutation, HandRouterStats},
    hands::{HandsMutation, HandsStats},
    holds::{HoldsMutation, HoldsStats},
    migrated::{MigratedMutation, MigratedStats},
    kvlease::{KvLeaseMutation, KvLeaseStats},
    scope::{ScopeMutation, ScopeStats},
    threadscope::{ThreadScopeMutation, ThreadScopeStats},
    httpd::WireMutation,
    httpserver::{HttpServerMutation, HttpServerStats},
    imports::{ImportsMutation, ImportsStats},
    inboxstress::{InboxStressMutation, InboxStressStats},
    heldawait::{HeldAwaitMutation, HeldAwaitStats},
    interpdrop::{InterpDropMutation, InterpDropStats},
    lease::{LeaseMutation, LeaseStats},
    live::{LiveMutation, LiveStats},
    longthread::{LongThreadMutation, LongThreadStats},
    model::Stats,
    mpp::{MppMutation, MppStats},
    nestedmeta::{NestedMetaMutation, NestedMetaStats},
    onelog::{LiveSeqs, Protections},
    onelogfacts::{OneLogFactsMutation, OneLogFactsStats},
    partitions::{PartitionsMutation, PartitionsStats},
    pgpool::{PgPoolMutation, PgPoolStats},
    plog::{PlogMutation, PlogStats},
    repocache::{RepoCacheMutation, RepoCacheStats},
    requestclose::RequestCloseStats,
    scale::{ScaleConfig, ScaleLoad, ScaleMutation, ScaleStats},
    secrets::{SecretsMutation, SecretsStats},
    simulate::{Options, run_seed},
    slog::{SlogMutation, SlogStats},
    append::{AppendMutation, AppendStats},
    steercarry::{CarryMutation, CarryStats},
    store::Mutation,
    streams::{StreamsMutation, StreamsStats},
    sync::{SyncMutation, SyncStats},
    tasks::{TasksMutation, TasksStats},
    turns::{Invariant, PolicyKind, TurnMutation, TurnOptions, TurnStats},
    wakes::{WakesMutation, WakesStats},
    websocket::{WebSocketMutation, WebSocketStats},
    whole::{ChildMode, StoreKind, WholeOptions, WholeStats, WireKind},
    workspacecrash::{WorkspaceCrashMutation, WorkspaceCrashStats},
    workspacefs::{WorkspaceFsMutation, WorkspaceFsStats},
    workspacehost::{WorkspaceHostMutation, WorkspaceHostStats},
    workspacesql::{WorkspaceSqlMutation, WorkspaceSqlStats},
    workspacestack::{WorkspaceStackMutation, WorkspaceStackStats},
};

const DEFAULT_OPS: u64 = 1000;
const CAUGHT_SHOWN_MAX: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    Store,
    Agents,
    Turns,
    Runtime,
    Websocket,
    HttpServer,
    Alloc,
    Egress,
    Mpp,
    Sync,
    Streams,
    Hands,
    CellCalls,
    Live,
    LongThread,
    RequestClose,
    SteerCarry,
    AntigravityDurable,
    InboxStress,
    Capture,
    Executor,
    Dns,
    Board,
    Controlplane,
    Cost,
    Plog,
    Secrets,
    Wakes,
    Imports,
    Facts,
    BackendFacts,
    BoardShards,
    HandRouter,
    WorkspaceHost,
    BoardSync,
    Dispatcher,
    BoardSteer,
    BoardBrief,
    WorkspaceFs,
    RepoCache,
    WorkspaceSql,
    GitRemote,
    WorkspaceStack,
    WorkspaceCrash,
    Partitions,
    Lease,
    Durable,
    Scale,
    HandAuto,
    NestedMeta,
    GcUnwind,
    InterpDrop,
    HeldAwait,
    Holds,
    Slog,
    Append,
    OneLogFacts,
    Migrated,
    KvLease,
    Scope,
    ThreadScope,
    PgPool,
}

struct Args {
    seeds: Range<u64>,
    options: Options,
    twice: bool,
    /// Print `SEED seed=N pass|fail` as each seed settles, for runners that time, schedule
    /// and recheck single seeds (bin/nanocode-sim/run.py).
    seed_lines: bool,
    /// Worker threads that run seeds at once (--jobs N); output is identical to --jobs 1.
    jobs: usize,
    scenario: Scenario,
    turns: TurnOptions,
    whole: WholeOptions,
    agents: AgentsMutation,
    websocket: WebSocketMutation,
    http_server: HttpServerMutation,
    mpp: MppMutation,
    sync: SyncMutation,
    streams: StreamsMutation,
    hands: HandsMutation,
    fleet: usize,
    reopen: bool,
    cancel: bool,
    cell_calls: CellCallsMutation,
    live: LiveMutation,
    long_thread: LongThreadMutation,
    tasks: TasksMutation,
    board: BoardMutation,
    controlplane: ControlplaneMutation,
    cost: CostMutation,
    plog: PlogMutation,
    secrets: SecretsMutation,
    wakes: WakesMutation,
    imports: ImportsMutation,
    facts: FactsMutation,
    backend_facts: BackendFactsMutation,
    board_shards: ShardsMutation,
    hand_router: HandRouterMutation,
    workspace_host: WorkspaceHostMutation,
    board_sync: BoardSyncMutation,
    dispatcher: DispatcherMutation,
    board_steer: BoardSteerMutation,
    steer_carry: CarryMutation,
    antigravity_durable: AntigravityMutation,
    inbox_stress: InboxStressMutation,
    board_brief: BoardBriefMutation,
    workspace_fs: WorkspaceFsMutation,
    repo_cache: RepoCacheMutation,
    workspace_sql: WorkspaceSqlMutation,
    git_remote: GitRemoteMutation,
    workspace_stack: WorkspaceStackMutation,
    workspace_crash: WorkspaceCrashMutation,
    partitions: PartitionsMutation,
    lease: LeaseMutation,
    durable: DurableMutation,
    scale: ScaleConfig,
    hand_auto: HandAutoMutation,
    nested_meta: NestedMetaMutation,
    gc_unwind: GcUnwindMutation,
    interp_drop: InterpDropMutation,
    held_await: HeldAwaitMutation,
    holds: HoldsMutation,
    slog: SlogMutation,
    append: AppendMutation,
    one_log_facts: OneLogFactsMutation,
    migrated: MigratedMutation,
    kv_lease: KvLeaseMutation,
    scope: ScopeMutation,
    thread_scope: ThreadScopeMutation,
    pg_pool: PgPoolMutation,
    #[cfg(unix)]
    alloc: alloc::AllocMutation,
    #[cfg(unix)]
    egress: egress::EgressMutation,
    #[cfg(unix)]
    capture: capture::CaptureMutation,
    #[cfg(target_os = "linux")]
    dns: dns::DnsMutation,
}

type Caught = BTreeMap<Invariant, (u64, u64, (u64, u64))>;

fn usage() -> ExitCode {
    eprintln!(
        "usage: nanocode-sim --tidy | --mutations | (--seed S | --seeds A..B) [--scenario store|agents|turns|runtime|websocket|http_server|alloc|egress|mpp|sync|streams|hands|cell_calls|long_thread|request_close|steer_carry|antigravity_durable|inbox_stress|capture|executor|dns|board|board_shards|controlplane|cost|plog|secrets|wakes|imports|facts|backend_facts|hand_router|workspace_host|board_sync|dispatcher|board_steer|board_brief|workspace_fs|repo_cache|workspace_sql|git_remote|workspace_stack|workspace_crash|live|partitions|lease|durable|scale|gc_unwind|interp_drop|held_await|holds|slog|append|one_log_facts|migrated|kv_lease|scope|thread_scope|pg_pool] [--ops N] [--twice] [--seed-lines] [--jobs N] [--mutation {}] [--records content|slots] [--verbose]\n       turns: [--mutation {}] [--without {}]... [--live-seqs temporary|floor|gap|reuse] [--calm] [--policy reference|turnlog] [--row-limit BYTES] [--reopen-row-limit BYTES] [--chain-bytes BYTES] [--long TURNS] [--snapshot-ratio K] [--snapshot-rows N] [--takeover] [--retain TURNS] [--state-bytes N] [--event-bytes N] [--owners N]\n       runtime: [--store memory|kv|log] [--deferred-start] [--calm] [--wire transport|http] [--children local|remote] [--mutation {}|{}]\n       agents: [--mutation {}]
       websocket: [--mutation {}]
       http_server: [--mutation {}]
       alloc: [--mutation overstate_size|forget_free|scribble]
       egress: [--mutation widen_allowlist|corrupt_tunnel_byte|rebind_on_connect|trust_private]
       mpp: [--mutation {}]
       sync: [--mutation {}]
       streams: [--mutation {}]
       hands: [--mutation {}] [--fleet N] [--reopen] [--cancel]
       cell_calls: [--mutation {}]
       live: [--mutation {}] [--trace-dir DIR]
       long_thread: [--mutation {}]
       capture: [--mutation skip_trim|drop_chunk|skip_redact|redact_then_trim]
       executor: [--mutation {}]
       dns: [--mutation cut_answer|drop_search]
       board: [--mutation {}]
       cost: [--mutation {}]
       plog: [--mutation {}]
       secrets: [--mutation {}]
       wakes: [--mutation {}]
       imports: [--mutation {}]
       facts: [--mutation {}]
       backend_facts: [--mutation {}]
       board_shards: [--mutation {}] | --board-shards-bench [RTT_US [ROW_US [BATCH [PARTITIONS [WRITERS]]]]]
       hand_router: [--mutation {}]
       board_sync: [--mutation {}]
       dispatcher: [--mutation {}]
       workspace_fs: [--mutation {}]
       repo_cache: [--mutation {}]
       workspace_sql: [--mutation {}]
       git_remote: [--mutation {}]
       workspace_stack: [--mutation {}]
       workspace_crash: [--mutation {}]
       partitions: [--mutation {}]
       lease: [--mutation {}]
       durable: [--mutation {}]
       scale: [--load uniform|skewed] [--nodes 1,2,..,10] [--mutation {}]
       hand_auto: [--mutation {}]
       nested_meta: [--mutation {}]
       gc_unwind: [--mutation {}]
       interp_drop: [--mutation {}]
       held_await: [--mutation {}]
       holds: [--mutation {}]
       slog: [--mutation {}]
       append: [--mutation {}]
       one_log_facts: [--mutation {}]
       migrated: [--mutation {}]
       kv_lease: [--mutation {}]
       scope: [--mutation {}]
       thread_scope: [--mutation {}]
       pg_pool: [--mutation {}]",
        Mutation::names(),
        TurnMutation::names(),
        onelog::PROTECTIONS.join("|"),
        WireMutation::NAMES,
        ChildMutation::NAMES,
        AgentsMutation::names(),
        WebSocketMutation::names(),
        HttpServerMutation::names(),
        MppMutation::names(),
        SyncMutation::names(),
        StreamsMutation::names(),
        HandsMutation::names(),
        CellCallsMutation::names(),
        LiveMutation::names(),
        LongThreadMutation::names(),
        TasksMutation::names(),
        BoardMutation::names(),
        CostMutation::names(),
        plog::names(),
        secrets::names(),
        wakes::names(),
        imports::names(),
        facts::names(),
        backendfacts::names(),
        ShardsMutation::names(),
        HandRouterMutation::names(),
        BoardSyncMutation::names(),
        DispatcherMutation::names(),
        WorkspaceFsMutation::names(),
        RepoCacheMutation::names(),
        WorkspaceSqlMutation::names(),
        GitRemoteMutation::names(),
        WorkspaceStackMutation::names(),
        WorkspaceCrashMutation::names(),
        PartitionsMutation::names(),
        LeaseMutation::names(),
        DurableMutation::names(),
        ScaleMutation::names(),
        HandAutoMutation::names(),
        NestedMetaMutation::names(),
        GcUnwindMutation::names(),
        InterpDropMutation::names(),
        HeldAwaitMutation::names(),
        HoldsMutation::names(),
        SlogMutation::names(),
        AppendMutation::names(),
        OneLogFactsMutation::names(),
        MigratedMutation::names(),
        KvLeaseMutation::names(),
        ScopeMutation::names(),
        ThreadScopeMutation::names(),
        PgPoolMutation::names()
    );
    ExitCode::from(2)
}

fn parse_seeds(value: &str) -> Option<Range<u64>> {
    if let Some((low, high)) = value.split_once("..") {
        return Some(low.parse().ok()?..high.parse().ok()?);
    }
    let seed: u64 = value.parse().ok()?;
    Some(seed..seed.checked_add(1)?)
}

fn parse_args(args: &[String]) -> Option<Args> {
    let mut seeds = None;
    let mut options = Options {
        ops: DEFAULT_OPS,
        mutation: Mutation::None,
        slots: false,
        verbose: false,
    };
    let mut twice = false;
    let mut jobs = 1_usize;
    let mut seed_lines = false;
    let mut scenario = Scenario::Store;
    let mut mutation = None;
    let mut turns = TurnOptions {
        calm: false,
        event_bytes: None,
        long: None,
        owners: None,
        retain: None,
        snapshot_ratio: 2,
        snapshot_rows: None,
        takeover: false,
        state_bytes: None,
        mutation: TurnMutation::None,
        policy: PolicyKind::Reference,
        row_limit: None,
        reopen_row_limit: None,
        chain_bytes: None,
        protections: Protections::ALL,
        verbose: false,
    };
    let mut whole = WholeOptions::default();
    let mut agents = AgentsMutation::None;
    let mut websocket = WebSocketMutation::None;
    let mut http_server = HttpServerMutation::None;
    let mut mpp = MppMutation::None;
    let mut sync = SyncMutation::None;
    let mut streams = StreamsMutation::None;
    let mut hands = HandsMutation::None;
    let mut fleet = 1_usize;
    let mut reopen = false;
    let mut cancel = false;
    let mut cell_calls = CellCallsMutation::None;
    let mut live = LiveMutation::None;
    let mut long_thread = LongThreadMutation::None;
    let mut tasks = TasksMutation::None;
    let mut board = BoardMutation::None;
    let mut controlplane = ControlplaneMutation::None;
    let mut cost = CostMutation::None;
    let mut plog = PlogMutation::None;
    let mut secrets = SecretsMutation::None;
    let mut wakes = WakesMutation::None;
    let mut imports = ImportsMutation::None;
    let mut facts = FactsMutation::None;
    let mut backend_facts = BackendFactsMutation::None;
    let mut board_shards = ShardsMutation::None;
    let mut hand_router = HandRouterMutation::None;
    let mut workspace_host = WorkspaceHostMutation::None;
    let mut board_sync = BoardSyncMutation::None;
    let mut dispatcher = DispatcherMutation::NONE;
    let mut board_steer = BoardSteerMutation::None;
    let mut steer_carry = CarryMutation::None;
    let mut antigravity_durable = AntigravityMutation::None;
    let mut inbox_stress = InboxStressMutation::None;
    let mut board_brief = BoardBriefMutation::None;
    let mut workspace_fs = WorkspaceFsMutation::None;
    let mut repo_cache = RepoCacheMutation::None;
    let mut workspace_sql = WorkspaceSqlMutation::None;
    let mut git_remote = GitRemoteMutation::None;
    let mut workspace_stack = WorkspaceStackMutation::None;
    let mut workspace_crash = WorkspaceCrashMutation::None;
    let mut partitions = PartitionsMutation::None;
    let mut lease = LeaseMutation::None;
    let mut durable = DurableMutation::None;
    let mut scale = ScaleConfig::default();
    let mut hand_auto = HandAutoMutation::None;
    let mut nested_meta = NestedMetaMutation::None;
    let mut gc_unwind = GcUnwindMutation::None;
    let mut interp_drop = InterpDropMutation::None;
    let mut held_await = HeldAwaitMutation::None;
    let mut holds = HoldsMutation::None;
    let mut slog = SlogMutation::None;
    let mut append = AppendMutation::None;
    let mut one_log_facts = OneLogFactsMutation::None;
    let mut migrated = MigratedMutation::None;
    let mut kv_lease = KvLeaseMutation::None;
    let mut scope = ScopeMutation::NONE;
    let mut thread_scope = ThreadScopeMutation::NONE;
    let mut pg_pool = PgPoolMutation::None;
    #[cfg(unix)]
    let mut capture = capture::CaptureMutation::None;
    #[cfg(target_os = "linux")]
    let mut dns = dns::DnsMutation::None;
    #[cfg(unix)]
    let mut alloc = alloc::AllocMutation::None;
    #[cfg(unix)]
    let mut egress = egress::EgressMutation::None;
    let mut rest = args.iter();
    while let Some(flag) = rest.next() {
        match flag.as_str() {
            "--twice" => twice = true,
            "--seed-lines" => seed_lines = true,
            "--jobs" => jobs = rest.next()?.parse().ok().filter(|&jobs| jobs > 0)?,
            "--trace-dir" => live::trace_to(rest.next()?),
            "--subagents-board" => boardagents::enable(),
            "--subagents-board-delayed" => boardagents::enable_delayed(),
            "--subagents-board-lagging" => boardagents::enable_lagging(),
            "--subagents-board-mutation" => {
                if !boardagents::mutate(rest.next()?) {
                    return None;
                }
            }
            "--verbose" => options.verbose = true,
            "--load" => scale.load = ScaleLoad::parse(rest.next()?)?,
            "--nodes" => scale.nodes = scale::parse_nodes(rest.next()?)?,
            "--calm" => turns.calm = true,
            "--store" => whole.store = Some(StoreKind::parse(rest.next()?)?),
            "--deferred-start" => whole.deferred_start = true,
            "--wire" => whole.wire = WireKind::parse(rest.next()?)?,
            "--children" => whole.children = ChildMode::parse(rest.next()?)?,
            "--policy" => turns.policy = PolicyKind::parse(rest.next()?)?,
            "--row-limit" => turns.row_limit = Some(rest.next()?.parse().ok()?),
            "--reopen-row-limit" => turns.reopen_row_limit = Some(rest.next()?.parse().ok()?),
            "--chain-bytes" => turns.chain_bytes = Some(rest.next()?.parse().ok()?),
            "--long" => turns.long = Some(rest.next()?.parse().ok()?),
            "--snapshot-ratio" => turns.snapshot_ratio = rest.next()?.parse().ok()?,
            "--snapshot-rows" => turns.snapshot_rows = Some(rest.next()?.parse().ok()?),
            "--takeover" => turns.takeover = true,
            "--retain" => turns.retain = Some(rest.next()?.parse().ok()?),
            "--state-bytes" => turns.state_bytes = Some(rest.next()?.parse().ok()?),
            "--event-bytes" => turns.event_bytes = Some(rest.next()?.parse().ok()?),
            "--owners" => turns.owners = Some(rest.next()?.parse().ok()?),
            "--reopen" => reopen = true,
            "--cancel" => cancel = true,
            "--fleet" => {
                fleet = rest
                    .next()?
                    .parse()
                    .ok()
                    .filter(|count: &usize| *count > 0)?
            }
            "--without" => turns.protections = turns.protections.without(rest.next()?)?,
            "--live-seqs" => turns.protections.live_seqs = LiveSeqs::parse(rest.next()?)?,
            "--seed" | "--seeds" => seeds = Some(parse_seeds(rest.next()?)?),
            "--ops" => options.ops = rest.next()?.parse().ok()?,
            "--scenario" => {
                scenario = match rest.next()?.as_str() {
                    "store" => Scenario::Store,
                    "agents" => Scenario::Agents,
                    "turns" => Scenario::Turns,
                    "runtime" => Scenario::Runtime,
                    "websocket" => Scenario::Websocket,
                    "http_server" => Scenario::HttpServer,
                    "mpp" => Scenario::Mpp,
                    "sync" => Scenario::Sync,
                    "streams" => Scenario::Streams,
                    "hands" => Scenario::Hands,
                    "cell_calls" => Scenario::CellCalls,
                    "live" => Scenario::Live,
                    "long_thread" => Scenario::LongThread,
                    "request_close" => Scenario::RequestClose,
                    "steer_carry" => Scenario::SteerCarry,
                    "antigravity_durable" => Scenario::AntigravityDurable,
                    "inbox_stress" => Scenario::InboxStress,
                    "executor" => Scenario::Executor,
                    "board" => Scenario::Board,
                    "controlplane" => Scenario::Controlplane,
                    "cost" => Scenario::Cost,
                    "plog" => Scenario::Plog,
                    "secrets" => Scenario::Secrets,
                    "wakes" => Scenario::Wakes,
                    "imports" => Scenario::Imports,
                    "facts" => Scenario::Facts,
                    "backend_facts" => Scenario::BackendFacts,
                    "board_shards" => Scenario::BoardShards,
                    "hand_router" => Scenario::HandRouter,
                    "workspace_host" => Scenario::WorkspaceHost,
                    "board_sync" => Scenario::BoardSync,
                    "dispatcher" => Scenario::Dispatcher,
                    "board_steer" => Scenario::BoardSteer,
                    "board_brief" => Scenario::BoardBrief,
                    "workspace_fs" => Scenario::WorkspaceFs,
                    "repo_cache" => Scenario::RepoCache,
                    "workspace_sql" => Scenario::WorkspaceSql,
                    "git_remote" => Scenario::GitRemote,
                    "workspace_stack" => Scenario::WorkspaceStack,
                    "workspace_crash" => Scenario::WorkspaceCrash,
                    "partitions" => Scenario::Partitions,
                    "lease" => Scenario::Lease,
                    "durable" => Scenario::Durable,
                    "scale" => Scenario::Scale,
                    "hand_auto" => Scenario::HandAuto,
                    "nested_meta" => Scenario::NestedMeta,
                    "gc_unwind" => Scenario::GcUnwind,
                    "interp_drop" => Scenario::InterpDrop,
                    "held_await" => Scenario::HeldAwait,
                    "holds" => Scenario::Holds,
                    "pg_pool" => Scenario::PgPool,
                    "slog" => Scenario::Slog,
                    "append" => Scenario::Append,
                    "one_log_facts" => Scenario::OneLogFacts,
                    "migrated" => Scenario::Migrated,
                    "kv_lease" => Scenario::KvLease,
                    "scope" => Scenario::Scope,
                    "thread_scope" => Scenario::ThreadScope,
                    #[cfg(unix)]
                    "capture" => Scenario::Capture,
                    #[cfg(target_os = "linux")]
                    "dns" => Scenario::Dns,
                    #[cfg(unix)]
                    "alloc" => Scenario::Alloc,
                    #[cfg(unix)]
                    "egress" => Scenario::Egress,
                    _ => return None,
                };
            }
            "--mutation" => mutation = Some(rest.next()?.as_str()),
            "--records" => {
                options.slots = match rest.next()?.as_str() {
                    "content" => false,
                    "slots" => true,
                    _ => return None,
                };
            }
            _ => return None,
        }
    }
    match (scenario, mutation) {
        (_, None) => {}
        (Scenario::Store, Some(name)) => options.mutation = Mutation::parse(name)?,
        (Scenario::Turns, Some(name)) => turns.mutation = TurnMutation::parse(name)?,
        (Scenario::Runtime, Some(name)) => match WireMutation::parse(name) {
            Some(mutation) => whole.mutation = mutation,
            None => match ChildMutation::parse(name) {
                Some(mutation) => whole.child_mutation = mutation,
                None => whole.store_mutation = Mutation::parse(name)?,
            },
        },
        (Scenario::Websocket, Some(name)) => websocket = WebSocketMutation::parse(name)?,
        (Scenario::HttpServer, Some(name)) => http_server = HttpServerMutation::parse(name)?,
        (Scenario::Mpp, Some(name)) => mpp = MppMutation::parse(name)?,
        (Scenario::Sync, Some(name)) => sync = SyncMutation::parse(name)?,
        (Scenario::Streams, Some(name)) => streams = StreamsMutation::parse(name)?,
        (Scenario::Hands, Some(name)) => hands = HandsMutation::parse(name)?,
        (Scenario::CellCalls, Some(name)) => cell_calls = CellCallsMutation::parse(name)?,
        (Scenario::Live, Some(name)) => live = LiveMutation::parse(name)?,
        (Scenario::LongThread, Some(name)) => long_thread = LongThreadMutation::parse(name)?,
        (Scenario::Executor, Some(name)) => tasks = TasksMutation::parse(name)?,
        (Scenario::RequestClose, Some(_)) => return None,
        (Scenario::SteerCarry, Some(name)) => steer_carry = CarryMutation::parse(name)?,
        (Scenario::AntigravityDurable, Some(name)) => {
            antigravity_durable = AntigravityMutation::parse(name)?;
        }
        (Scenario::InboxStress, Some(name)) => inbox_stress = InboxStressMutation::parse(name)?,
        (Scenario::Board, Some(name)) => board = BoardMutation::parse(name)?,
        (Scenario::Controlplane, Some(name)) => {
            controlplane = ControlplaneMutation::parse(name)?;
        }
        (Scenario::Cost, Some(name)) => cost = CostMutation::parse(name)?,
        (Scenario::Plog, Some(name)) => plog = PlogMutation::parse(name)?,
        (Scenario::Secrets, Some(name)) => secrets = SecretsMutation::parse(name)?,
        (Scenario::Wakes, Some(name)) => wakes = WakesMutation::parse(name)?,
        (Scenario::Imports, Some(name)) => imports = ImportsMutation::parse(name)?,
        (Scenario::Facts, Some(name)) => facts = FactsMutation::parse(name)?,
        (Scenario::BackendFacts, Some(name)) => {
            backend_facts = BackendFactsMutation::parse(name)?;
        }
        (Scenario::BoardShards, Some(name)) => board_shards = ShardsMutation::parse(name)?,
        (Scenario::HandRouter, Some(name)) => hand_router = HandRouterMutation::parse(name)?,
        (Scenario::Partitions, Some(name)) => partitions = PartitionsMutation::parse(name)?,
        (Scenario::Lease, Some(name)) => lease = LeaseMutation::parse(name)?,
        (Scenario::Durable, Some(name)) => durable = DurableMutation::parse(name)?,
        (Scenario::Scale, Some(name)) => scale.mutation = ScaleMutation::parse(name)?,
        (Scenario::WorkspaceHost, Some(name)) => {
            workspace_host = WorkspaceHostMutation::parse(name)?
        }
        (Scenario::BoardSync, Some(name)) => board_sync = BoardSyncMutation::parse(name)?,
        (Scenario::Dispatcher, Some(name)) => dispatcher = DispatcherMutation::parse(name)?,
        (Scenario::BoardSteer, Some(name)) => board_steer = BoardSteerMutation::parse(name)?,
        (Scenario::BoardBrief, Some(name)) => board_brief = BoardBriefMutation::parse(name)?,
        (Scenario::WorkspaceFs, Some(name)) => workspace_fs = WorkspaceFsMutation::parse(name)?,
        (Scenario::RepoCache, Some(name)) => repo_cache = RepoCacheMutation::parse(name)?,
        (Scenario::WorkspaceSql, Some(name)) => workspace_sql = WorkspaceSqlMutation::parse(name)?,
        (Scenario::GitRemote, Some(name)) => git_remote = GitRemoteMutation::parse(name)?,
        (Scenario::WorkspaceStack, Some(name)) => {
            workspace_stack = WorkspaceStackMutation::parse(name)?
        }
        (Scenario::WorkspaceCrash, Some(name)) => {
            workspace_crash = WorkspaceCrashMutation::parse(name)?
        }
        (Scenario::HandAuto, Some(name)) => hand_auto = HandAutoMutation::parse(name)?,
        (Scenario::NestedMeta, Some(name)) => nested_meta = NestedMetaMutation::parse(name)?,
        (Scenario::GcUnwind, Some(name)) => gc_unwind = GcUnwindMutation::parse(name)?,
        (Scenario::InterpDrop, Some(name)) => interp_drop = InterpDropMutation::parse(name)?,
        (Scenario::HeldAwait, Some(name)) => held_await = HeldAwaitMutation::parse(name)?,
        (Scenario::Holds, Some(name)) => holds = HoldsMutation::parse(name)?,
        (Scenario::Migrated, Some(name)) => migrated = MigratedMutation::parse(name)?,
        (Scenario::KvLease, Some(name)) => kv_lease = KvLeaseMutation::parse(name)?,
        (Scenario::Scope, Some(name)) => scope = ScopeMutation::parse(name)?,
        (Scenario::ThreadScope, Some(name)) => {
            thread_scope = ThreadScopeMutation::parse(name)?;
        }
        (Scenario::PgPool, Some(name)) => pg_pool = PgPoolMutation::parse(name)?,
        (Scenario::Slog, Some(name)) => slog = SlogMutation::parse(name)?,
        (Scenario::Append, Some(name)) => append = AppendMutation::parse(name)?,
        (Scenario::OneLogFacts, Some(name)) => {
            one_log_facts = OneLogFactsMutation::parse(name)?;
        }
        #[cfg(unix)]
        (Scenario::Capture, Some(name)) => capture = capture::CaptureMutation::parse(name)?,
        #[cfg(not(unix))]
        (Scenario::Capture, Some(_)) => return None,
        #[cfg(target_os = "linux")]
        (Scenario::Dns, Some(name)) => dns = dns::DnsMutation::parse(name)?,
        #[cfg(not(target_os = "linux"))]
        (Scenario::Dns, Some(_)) => return None,
        #[cfg(unix)]
        (Scenario::Alloc, Some(name)) => alloc = alloc::AllocMutation::parse(name)?,
        #[cfg(unix)]
        (Scenario::Egress, Some(name)) => egress = egress::EgressMutation::parse(name)?,
        #[cfg(not(unix))]
        (Scenario::Alloc, Some(_)) => return None,
        #[cfg(not(unix))]
        (Scenario::Egress, Some(_)) => return None,
        (Scenario::Agents, Some(name)) => agents = AgentsMutation::parse(name)?,
    }
    if whole.store_mutation != Mutation::None && whole.store != Some(StoreKind::Kv) {
        return None;
    }
    if jobs > 1 && options.verbose {
        return None;
    }
    turns.verbose = options.verbose;
    whole.verbose = options.verbose;
    whole.calm = turns.calm;
    Some(Args {
        seeds: seeds?,
        options,
        twice,
        seed_lines,
        jobs,
        scenario,
        turns,
        whole,
        agents,
        websocket,
        http_server,
        mpp,
        sync,
        streams,
        hands,
        fleet,
        reopen,
        cancel,
        cell_calls,
        live,
        long_thread,
        tasks,
        board,
        controlplane,
        cost,
        plog,
        secrets,
        wakes,
        imports,
        facts,
        backend_facts,
        board_shards,
        hand_router,
        workspace_host,
        board_sync,
        dispatcher,
        board_steer,
        steer_carry,
        antigravity_durable,
        inbox_stress,
        board_brief,
        workspace_fs,
        repo_cache,
        workspace_sql,
        git_remote,
        workspace_stack,
        workspace_crash,
        partitions,
        lease,
        durable,
        scale,
        hand_auto,
        nested_meta,
        gc_unwind,
        interp_drop,
        held_await,
        holds,
        slog,
        append,
        one_log_facts,
        migrated,
        kv_lease,
        scope,
        thread_scope,
        pg_pool,
        #[cfg(unix)]
        alloc,
        #[cfg(unix)]
        egress,
        #[cfg(unix)]
        capture,
        #[cfg(target_os = "linux")]
        dns,
    })
}

fn controlplane_agents(args: &Args) -> Option<u64> {
    (args.options.ops != DEFAULT_OPS).then_some(args.options.ops)
}

fn replay(seed: u64, args: &Args) -> String {
    let mut command = scenario_replay(seed, args);
    command.push_str(&boardagents::replay_flags());
    command
}

fn scenario_replay(seed: u64, args: &Args) -> String {
    let options = args.options;
    if args.scenario == Scenario::Websocket {
        return websocket::replay(seed, args.websocket);
    }
    if args.scenario == Scenario::HttpServer {
        return httpserver::replay(seed, args.http_server);
    }
    if args.scenario == Scenario::Mpp {
        return mpp::replay(seed, args.mpp);
    }
    if args.scenario == Scenario::Sync {
        return sync::replay(seed, args.sync);
    }
    if args.scenario == Scenario::Streams {
        return streams::replay(seed, args.streams);
    }
    if args.scenario == Scenario::Hands {
        return hands::replay(
            seed,
            args.hands,
            hands::HandsOptions {
                fleet: args.fleet,
                reopen: args.reopen,
                cancel: args.cancel,
            },
        );
    }
    if args.scenario == Scenario::CellCalls {
        return cellcalls::replay(seed, args.cell_calls);
    }
    if args.scenario == Scenario::Live {
        return live::replay(seed, args.live);
    }
    if args.scenario == Scenario::LongThread {
        return longthread::replay(seed, args.long_thread);
    }
    if args.scenario == Scenario::RequestClose {
        return requestclose::replay(seed);
    }
    if args.scenario == Scenario::SteerCarry {
        return steercarry::replay(seed, args.steer_carry);
    }
    if args.scenario == Scenario::AntigravityDurable {
        return antigravitydurable::replay(seed, args.antigravity_durable);
    }
    if args.scenario == Scenario::InboxStress {
        return inboxstress::replay(seed, args.inbox_stress);
    }
    if args.scenario == Scenario::Agents {
        let mut command = format!("nanocode-sim --scenario agents --seed {seed}");
        if args.agents != AgentsMutation::None {
            command.push_str(" --mutation ");
            command.push_str(args.agents.name());
        }
        return command;
    }
    if args.scenario == Scenario::Executor {
        return tasks::replay(seed, args.tasks);
    }
    if args.scenario == Scenario::Board {
        return board::replay(seed, args.board);
    }
    if args.scenario == Scenario::Controlplane {
        return controlplane::replay(seed, controlplane_agents(args), args.controlplane);
    }
    if args.scenario == Scenario::BoardShards {
        return board_shards::replay(seed, args.board_shards);
    }
    if args.scenario == Scenario::Cost {
        return cost::replay(seed, args.cost);
    }
    if args.scenario == Scenario::Plog {
        return plog::replay(seed, args.plog);
    }
    if args.scenario == Scenario::Secrets {
        return secrets::replay(seed, args.secrets);
    }
    if args.scenario == Scenario::Wakes {
        return wakes::replay(seed, args.wakes);
    }
    if args.scenario == Scenario::Imports {
        return imports::replay(seed, args.imports);
    }
    if args.scenario == Scenario::Facts {
        return facts::replay(seed, args.facts);
    }
    if args.scenario == Scenario::BackendFacts {
        return backendfacts::replay(seed, args.backend_facts);
    }
    if args.scenario == Scenario::HandRouter {
        return handrouter::replay(seed, args.hand_router);
    }
    if args.scenario == Scenario::WorkspaceHost {
        return workspacehost::replay(seed, args.workspace_host);
    }
    if args.scenario == Scenario::GitRemote {
        return gitremote::replay(seed, args.git_remote);
    }
    if args.scenario == Scenario::WorkspaceStack {
        return workspacestack::replay(seed, args.workspace_stack);
    }
    if args.scenario == Scenario::WorkspaceCrash {
        return workspacecrash::replay(seed, args.workspace_crash);
    }
    if args.scenario == Scenario::Partitions {
        return partitions::replay(seed, args.partitions);
    }
    if args.scenario == Scenario::Lease {
        return lease::replay(seed, args.lease);
    }
    if args.scenario == Scenario::Durable {
        return durable::replay(seed, args.durable);
    }
    if args.scenario == Scenario::Scale {
        return scale::replay(seed, &args.scale);
    }
    if args.scenario == Scenario::HandAuto {
        return handauto::replay(seed, args.hand_auto);
    }
    if args.scenario == Scenario::NestedMeta {
        return nestedmeta::replay(seed, args.nested_meta);
    }
    if args.scenario == Scenario::GcUnwind {
        return gcunwind::replay(seed, args.gc_unwind);
    }
    if args.scenario == Scenario::InterpDrop {
        return interpdrop::replay(seed, args.interp_drop);
    }
    if args.scenario == Scenario::HeldAwait {
        return heldawait::replay(seed, args.held_await);
    }
    if args.scenario == Scenario::Holds {
        return holds::replay(seed, args.holds);
    }
    if args.scenario == Scenario::Migrated {
        return migrated::replay(seed, args.migrated);
    }
    if args.scenario == Scenario::KvLease {
        return kvlease::replay(seed, args.kv_lease);
    }
    if args.scenario == Scenario::Scope {
        return scope::replay(seed, args.scope);
    }
    if args.scenario == Scenario::ThreadScope {
        return threadscope::replay(seed, args.thread_scope);
    }
    if args.scenario == Scenario::PgPool {
        return pgpool::replay(seed, args.pg_pool);
    }
    if args.scenario == Scenario::Slog {
        return slog::replay(seed, args.slog);
    }
    if args.scenario == Scenario::Append {
        return append::replay(seed, args.append);
    }
    if args.scenario == Scenario::OneLogFacts {
        return onelogfacts::replay(seed, args.one_log_facts);
    }
    if args.scenario == Scenario::BoardSync {
        return board_sync::replay(seed, args.board_sync);
    }
    if args.scenario == Scenario::Dispatcher {
        return dispatcher::replay(seed, args.dispatcher);
    }
    if args.scenario == Scenario::BoardSteer {
        return board_steer::replay(seed, args.board_steer);
    }
    if args.scenario == Scenario::BoardBrief {
        return board_brief::replay(seed, args.board_brief);
    }
    if args.scenario == Scenario::WorkspaceFs {
        return workspacefs::replay(seed, args.workspace_fs);
    }
    if args.scenario == Scenario::RepoCache {
        return repocache::replay(seed, args.repo_cache);
    }
    if args.scenario == Scenario::WorkspaceSql {
        return workspacesql::replay(seed, args.workspace_sql);
    }
    #[cfg(unix)]
    if args.scenario == Scenario::Capture {
        return capture::replay(seed, args.capture);
    }
    #[cfg(target_os = "linux")]
    if args.scenario == Scenario::Dns {
        return dns::replay(seed, args.dns);
    }
    #[cfg(unix)]
    if args.scenario == Scenario::Alloc {
        return alloc::replay(seed, options.ops, args.alloc);
    }
    #[cfg(unix)]
    if args.scenario == Scenario::Egress {
        return egress::replay(seed, args.egress);
    }
    if args.scenario == Scenario::Runtime {
        let mut command = format!("nanocode-sim --scenario runtime --seed {seed}");
        if let Some(store) = args.whole.store {
            command.push_str(" --store ");
            command.push_str(store.name());
        }
        if args.whole.deferred_start {
            command.push_str(" --deferred-start");
        }
        if args.whole.calm {
            command.push_str(" --calm");
        }
        if args.whole.wire != WireKind::Transport {
            command.push_str(" --wire ");
            command.push_str(args.whole.wire.name());
        }
        if args.whole.children != ChildMode::Local {
            command.push_str(" --children ");
            command.push_str(args.whole.children.name());
        }
        if args.whole.mutation != WireMutation::None {
            command.push_str(" --mutation ");
            command.push_str(args.whole.mutation.name());
        }
        if args.whole.child_mutation != ChildMutation::None {
            command.push_str(" --mutation ");
            command.push_str(args.whole.child_mutation.name());
        }
        if args.whole.store_mutation != Mutation::None {
            command.push_str(" --mutation ");
            command.push_str(args.whole.store_mutation.name());
        }
        return command;
    }
    if args.scenario == Scenario::Turns {
        let mut command = format!("nanocode-sim --scenario turns --seed {seed}");
        if args.turns.calm {
            command.push_str(" --calm");
        }
        if args.turns.policy != PolicyKind::Reference {
            command.push_str(" --policy ");
            command.push_str(args.turns.policy.name());
        }
        if let Some(limit) = args.turns.row_limit {
            command.push_str(&format!(" --row-limit {limit}"));
        }
        if let Some(limit) = args.turns.reopen_row_limit {
            command.push_str(&format!(" --reopen-row-limit {limit}"));
        }
        if let Some(bytes) = args.turns.chain_bytes {
            command.push_str(&format!(" --chain-bytes {bytes}"));
        }
        if let Some(turns) = args.turns.long {
            command.push_str(&format!(" --long {turns}"));
        }
        if args.turns.snapshot_ratio != 2 {
            command.push_str(&format!(" --snapshot-ratio {}", args.turns.snapshot_ratio));
        }
        if let Some(rows) = args.turns.snapshot_rows {
            command.push_str(&format!(" --snapshot-rows {rows}"));
        }
        if args.turns.takeover {
            command.push_str(" --takeover");
        }
        if let Some(keep) = args.turns.retain {
            command.push_str(&format!(" --retain {keep}"));
        }
        if let Some(bytes) = args.turns.state_bytes {
            command.push_str(&format!(" --state-bytes {bytes}"));
        }
        if let Some(bytes) = args.turns.event_bytes {
            command.push_str(&format!(" --event-bytes {bytes}"));
        }
        if let Some(owners) = args.turns.owners {
            command.push_str(&format!(" --owners {owners}"));
        }
        for name in args.turns.protections.off() {
            command.push_str(" --without ");
            command.push_str(name);
        }
        if args.turns.protections.live_seqs != LiveSeqs::Temporary {
            command.push_str(" --live-seqs ");
            command.push_str(args.turns.protections.live_seqs.name());
        }
        if args.turns.mutation != TurnMutation::None {
            command.push_str(" --mutation ");
            command.push_str(args.turns.mutation.name());
        }
        return command;
    }
    let mut command = format!("nanocode-sim --seed {seed} --ops {}", options.ops);
    if options.mutation != Mutation::None {
        command.push_str(" --mutation ");
        command.push_str(options.mutation.name());
    }
    if options.slots {
        command.push_str(" --records slots");
    }
    command
}

fn judge(seed: u64, args: &Args, totals: &mut Stats) -> Option<String> {
    let first = run_seed(seed, args.options);
    totals.add(&first.stats);
    if args.options.verbose {
        println!("seed {seed} digest {:016x} {}", first.digest, first.shape);
    }
    if first.wrong.is_some() || !args.twice {
        return first
            .wrong
            .map(|message| format!("{} :: {message}", first.shape));
    }
    let quiet = Options {
        verbose: false,
        ..args.options
    };
    let second = crate::host::second_pass(|| run_seed(seed, quiet));
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "{} :: nondeterminism: trace digest {:016x} then {:016x}",
            first.shape, first.digest, second.digest
        )
    })
}

fn judge_agents(seed: u64, args: &Args, totals: &mut AgentStats) -> Option<String> {
    boardagents::begin(seed);
    let mut first = agents::run_seed(seed, args.agents);
    if let Some(error) = boardagents::finish() {
        first.wrong.get_or_insert(error);
    }
    totals.add(&first.stats);
    if args.options.verbose {
        println!("seed {seed} digest {:016x} {}", first.digest, first.shape);
    }
    if first.wrong.is_some() || !args.twice {
        return first
            .wrong
            .map(|message| format!("{} :: {message}", first.shape));
    }
    let second = crate::host::second_pass(|| {
        boardagents::begin(seed);
        let mut second = agents::run_seed(seed, args.agents);
        if let Some(error) = boardagents::finish() {
            second.wrong.get_or_insert(error);
        }
        second
    });
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "{} :: nondeterminism: trace digest {:016x} then {:016x}",
            first.shape, first.digest, second.digest
        )
    })
}

fn judge_whole(seed: u64, args: &Args, totals: &mut WholeStats) -> Option<String> {
    boardagents::begin(seed);
    let mut first = whole::run_seed(seed, args.whole);
    if let Some(error) = boardagents::finish() {
        first.wrong.get_or_insert(error);
    }
    totals.add(&first.stats);
    if args.options.verbose {
        println!("seed {seed} digest {:016x} {}", first.digest, first.shape);
    }
    if first.wrong.is_some() || !args.twice {
        return first
            .wrong
            .map(|message| format!("{} :: {message}", first.shape));
    }
    let quiet = WholeOptions {
        verbose: false,
        ..args.whole
    };
    let second = crate::host::second_pass(|| {
        boardagents::begin(seed);
        let mut second = whole::run_seed(seed, quiet);
        if let Some(error) = boardagents::finish() {
            second.wrong.get_or_insert(error);
        }
        second
    });
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "{} :: nondeterminism: trace digest {:016x} then {:016x}, {}",
            first.shape,
            first.digest,
            second.digest,
            whole::divergence(&first.lines, &second.lines)
        )
    })
}

fn judge_turns(
    seed: u64,
    args: &Args,
    totals: &mut TurnStats,
    caught: &mut Caught,
) -> Option<String> {
    let first = turns::run_seed(seed, args.turns);
    totals.add(&first.stats);
    if args.options.verbose {
        println!("seed {seed} digest {:016x} {}", first.digest, first.shape);
    }
    if let Some((invariant, message)) = &first.wrong {
        let entry = caught
            .entry(*invariant)
            .or_insert((0, seed, (first.lines, seed)));
        entry.0 += 1;
        if first.lines < entry.2.0 {
            entry.2 = (first.lines, seed);
        }
        return Some(format!(
            "{} :: {}: {message}",
            first.shape,
            invariant.name()
        ));
    }
    if !args.twice {
        return None;
    }
    let quiet = TurnOptions {
        verbose: false,
        ..args.turns
    };
    let second = crate::host::second_pass(|| turns::run_seed(seed, quiet));
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "{} :: nondeterminism: trace digest {:016x} then {:016x}",
            first.shape, first.digest, second.digest
        )
    })
}

fn report_turns(
    args: &Args,
    passed: u64,
    failed: u64,
    totals: &TurnStats,
    caught: &Caught,
) -> ExitCode {
    for (invariant, (count, first, (lines, shortest))) in caught {
        println!(
            "INVARIANT {} failed={count} first_seed={first} shortest_seed={shortest} trace_lines={lines}",
            invariant.name()
        );
    }
    println!(
        "RESULT seeds={}..{} scenario=turns policy={} calm={} protections_off={:?} live_seqs={} mutation={} passed={passed} failed={failed} twice={} {} stats={totals:?}",
        args.seeds.start,
        args.seeds.end,
        args.turns.policy.name(),
        args.turns.calm,
        args.turns.protections.off(),
        args.turns.protections.live_seqs.name(),
        args.turns.mutation.name(),
        args.twice,
        totals.summary()
    );
    let mutation = args.turns.mutation;
    if mutation != TurnMutation::None {
        if failed > 0 {
            println!("MUTATION {} caught by {failed} seeds", mutation.name());
            return ExitCode::SUCCESS;
        }
        println!("MUTATION {} NOT caught", mutation.name());
        return ExitCode::FAILURE;
    }
    if args.turns.protections != Protections::ALL || failed == 0 {
        return ExitCode::SUCCESS;
    }
    ExitCode::FAILURE
}

#[cfg(unix)]
fn alloc_experiment(args: &Args) -> bool {
    (args.scenario == Scenario::Alloc && args.alloc != alloc::AllocMutation::None)
        || (args.scenario == Scenario::Egress && args.egress != egress::EgressMutation::None)
        || (args.scenario == Scenario::Capture && args.capture != capture::CaptureMutation::None)
}

#[cfg(not(unix))]
const fn alloc_experiment(_args: &Args) -> bool {
    false
}

const fn mutation_name(whole: &WholeOptions) -> &'static str {
    match (whole.child_mutation, whole.store_mutation) {
        (ChildMutation::None, Mutation::None) => whole.mutation.name(),
        (ChildMutation::None, mutation) => mutation.name(),
        (mutation, _) => mutation.name(),
    }
}

fn mutations() -> ExitCode {
    let mut lines = Vec::new();
    let mut add = |scenario: &str, names: String| {
        for name in names
            .split('|')
            .filter(|name| !name.is_empty() && *name != "none")
        {
            lines.push(format!("MUTATION scenario={scenario} mutation={name}"));
        }
    };
    add("store", Mutation::store_names());
    add("turns", TurnMutation::names());
    add("runtime", WireMutation::NAMES.to_owned());
    add("runtime", ChildMutation::NAMES.to_owned());
    add("runtime", Mutation::runtime_names());
    add("agents", AgentsMutation::names());
    add("websocket", WebSocketMutation::names());
    add("http_server", HttpServerMutation::names());
    add("mpp", MppMutation::names());
    add("sync", SyncMutation::names());
    add("streams", StreamsMutation::names());
    add("hands", HandsMutation::names());
    add("cell_calls", CellCallsMutation::names());
    add("live", LiveMutation::names());
    add("long_thread", LongThreadMutation::names());
    add("executor", TasksMutation::names());
    add("board", BoardMutation::names());
    add("controlplane", ControlplaneMutation::names());
    add("cost", CostMutation::names());
    add("plog", plog::names());
    add("secrets", secrets::names());
    add("wakes", wakes::names());
    add("imports", imports::names());
    add("facts", facts::names());
    add("backend_facts", backendfacts::names());
    add("board_shards", ShardsMutation::names());
    add("hand_router", HandRouterMutation::names());
    add("workspace_host", WorkspaceHostMutation::names());
    add("board_sync", BoardSyncMutation::names());
    add("dispatcher", DispatcherMutation::names());
    add("board_steer", BoardSteerMutation::names());
    add("steer_carry", CarryMutation::names());
    add("antigravity_durable", AntigravityMutation::names());
    add("inbox_stress", InboxStressMutation::names());
    add("board_brief", BoardBriefMutation::names());
    add("workspace_fs", WorkspaceFsMutation::names());
    add("repo_cache", RepoCacheMutation::names());
    add("workspace_sql", WorkspaceSqlMutation::names());
    add("git_remote", GitRemoteMutation::names());
    add("workspace_stack", WorkspaceStackMutation::names());
    add("workspace_crash", WorkspaceCrashMutation::names());
    add("partitions", PartitionsMutation::names());
    add("lease", LeaseMutation::names());
    add("durable", DurableMutation::names());
    add("scale", ScaleMutation::names());
    add("hand_auto", HandAutoMutation::names());
    add("nested_meta", NestedMetaMutation::names());
    add("gc_unwind", GcUnwindMutation::names());
    add("interp_drop", InterpDropMutation::names());
    add("held_await", HeldAwaitMutation::names());
    add("holds", HoldsMutation::names());
    add("slog", SlogMutation::names());
    add("append", AppendMutation::names());
    add("one_log_facts", OneLogFactsMutation::names());
    add("migrated", MigratedMutation::names());
    add("kv_lease", KvLeaseMutation::names());
    add("scope", ScopeMutation::names());
    add("thread_scope", ThreadScopeMutation::names());
    add("pg_pool", PgPoolMutation::names());
    #[cfg(unix)]
    {
        add("alloc", alloc::AllocMutation::names());
        add("egress", egress::EgressMutation::names());
        add("capture", capture::CaptureMutation::names());
    }
    #[cfg(target_os = "linux")]
    add("dns", dns::DnsMutation::names());
    for line in lines {
        println!("{line}");
    }
    ExitCode::SUCCESS
}

fn tidy() -> ExitCode {
    match tidy::check(&tidy::workspace_root()) {
        Ok(problems) if problems.is_empty() => {
            println!("TIDY determinism: clean");
            ExitCode::SUCCESS
        }
        Ok(problems) => {
            for problem in problems {
                println!("TIDY {problem}");
            }
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("TIDY could not read the sources: {error}");
            ExitCode::FAILURE
        }
    }
}

#[derive(Default)]
struct Totals {
    totals: Stats,
    agent_totals: AgentStats,
    turn_totals: TurnStats,
    whole_totals: WholeStats,
    websocket_totals: WebSocketStats,
    http_server_totals: HttpServerStats,
    mpp_totals: MppStats,
    sync_totals: SyncStats,
    streams_totals: StreamsStats,
    hands_totals: HandsStats,
    cell_calls_totals: CellCallsStats,
    live_totals: LiveStats,
    long_thread_totals: LongThreadStats,
    request_close_totals: RequestCloseStats,
    steer_carry_totals: CarryStats,
    antigravity_durable_totals: AntigravityStats,
    inbox_stress_totals: InboxStressStats,
    tasks_totals: TasksStats,
    board_totals: BoardStats,
    controlplane_totals: ControlplaneStats,
    cost_totals: CostStats,
    plog_totals: PlogStats,
    secrets_totals: SecretsStats,
    wakes_totals: WakesStats,
    imports_totals: ImportsStats,
    shards_totals: ShardsStats,
    hand_router_totals: HandRouterStats,
    workspace_host_totals: WorkspaceHostStats,
    board_sync_totals: BoardSyncStats,
    dispatcher_totals: DispatcherStats,
    board_steer_totals: BoardSteerStats,
    board_brief_totals: BoardBriefStats,
    workspace_fs_totals: WorkspaceFsStats,
    repo_cache_totals: RepoCacheStats,
    workspace_sql_totals: WorkspaceSqlStats,
    git_remote_totals: GitRemoteStats,
    workspace_stack_totals: WorkspaceStackStats,
    workspace_crash_totals: WorkspaceCrashStats,
    partitions_totals: PartitionsStats,
    lease_totals: LeaseStats,
    durable_totals: DurableStats,
    scale_totals: ScaleStats,
    hand_auto_totals: HandAutoStats,
    nested_meta_totals: NestedMetaStats,
    gc_unwind_totals: GcUnwindStats,
    interp_drop_totals: InterpDropStats,
    held_await_totals: HeldAwaitStats,
    holds_totals: HoldsStats,
    slog_totals: SlogStats,
    append_totals: AppendStats,
    one_log_facts_totals: OneLogFactsStats,
    migrated_totals: MigratedStats,
    kv_lease_totals: KvLeaseStats,
    scope_totals: ScopeStats,
    thread_scope_totals: ThreadScopeStats,
    pg_pool_totals: PgPoolStats,
    #[cfg(unix)]
    capture_totals: capture::CaptureStats,
    #[cfg(target_os = "linux")]
    dns_totals: dns::DnsStats,
    #[cfg(unix)]
    alloc_totals: alloc::AllocStats,
    #[cfg(unix)]
    egress_totals: egress::EgressStats,
}

impl Totals {
    fn add(&mut self, other: &Self) {
        self.totals.add(&other.totals);
        self.agent_totals.add(&other.agent_totals);
        self.turn_totals.add(&other.turn_totals);
        self.whole_totals.add(&other.whole_totals);
        self.websocket_totals.add(&other.websocket_totals);
        self.http_server_totals.add(&other.http_server_totals);
        self.mpp_totals.add(&other.mpp_totals);
        self.sync_totals.add(&other.sync_totals);
        self.streams_totals.add(&other.streams_totals);
        self.hands_totals.add(&other.hands_totals);
        self.cell_calls_totals.add(&other.cell_calls_totals);
        self.live_totals.add(&other.live_totals);
        self.long_thread_totals.add(&other.long_thread_totals);
        self.request_close_totals.add(&other.request_close_totals);
        self.steer_carry_totals.add(&other.steer_carry_totals);
        self.antigravity_durable_totals
            .add(&other.antigravity_durable_totals);
        self.inbox_stress_totals.add(&other.inbox_stress_totals);
        self.tasks_totals.add(&other.tasks_totals);
        self.board_totals.add(&other.board_totals);
        self.controlplane_totals.add(&other.controlplane_totals);
        self.cost_totals.add(&other.cost_totals);
        self.plog_totals.add(&other.plog_totals);
        self.secrets_totals.add(&other.secrets_totals);
        self.wakes_totals.add(&other.wakes_totals);
        self.imports_totals.add(&other.imports_totals);
        self.shards_totals.add(&other.shards_totals);
        self.hand_router_totals.add(&other.hand_router_totals);
        self.workspace_host_totals.add(&other.workspace_host_totals);
        self.board_sync_totals.add(&other.board_sync_totals);
        self.dispatcher_totals.add(&other.dispatcher_totals);
        self.board_steer_totals.add(&other.board_steer_totals);
        self.board_brief_totals.add(&other.board_brief_totals);
        self.workspace_fs_totals.add(&other.workspace_fs_totals);
        self.repo_cache_totals.add(&other.repo_cache_totals);
        self.workspace_sql_totals.add(&other.workspace_sql_totals);
        self.git_remote_totals.add(&other.git_remote_totals);
        self.workspace_stack_totals
            .add(&other.workspace_stack_totals);
        self.workspace_crash_totals
            .add(&other.workspace_crash_totals);
        self.partitions_totals.add(&other.partitions_totals);
        self.lease_totals.add(&other.lease_totals);
        self.durable_totals.add(&other.durable_totals);
        self.scale_totals.add(&other.scale_totals);
        self.hand_auto_totals.add(&other.hand_auto_totals);
        self.nested_meta_totals.add(&other.nested_meta_totals);
        self.gc_unwind_totals.add(&other.gc_unwind_totals);
        self.interp_drop_totals.add(&other.interp_drop_totals);
        self.held_await_totals.add(&other.held_await_totals);
        self.holds_totals.add(&other.holds_totals);
        self.slog_totals.add(&other.slog_totals);
        self.append_totals.add(&other.append_totals);
        self.one_log_facts_totals.add(&other.one_log_facts_totals);
        self.migrated_totals.add(&other.migrated_totals);
        self.kv_lease_totals.add(&other.kv_lease_totals);
        self.scope_totals.add(&other.scope_totals);
        self.thread_scope_totals.add(&other.thread_scope_totals);
        self.pg_pool_totals.add(&other.pg_pool_totals);
        #[cfg(unix)]
        self.capture_totals.add(&other.capture_totals);
        #[cfg(target_os = "linux")]
        self.dns_totals.add(&other.dns_totals);
        #[cfg(unix)]
        self.alloc_totals.add(&other.alloc_totals);
        #[cfg(unix)]
        self.egress_totals.add(&other.egress_totals);
    }
}

fn merge_caught(caught: &mut Caught, seed: &Caught) {
    for (invariant, (count, first, shortest)) in seed {
        let entry = caught.entry(*invariant).or_insert((0, *first, *shortest));
        entry.0 += count;
        if shortest.0 < entry.2.0 {
            entry.2 = *shortest;
        }
    }
}

/// Judges one seed of the chosen scenario, adding its stats to totals.
fn judge_one(seed: u64, args: &Args, totals: &mut Totals, caught: &mut Caught) -> Option<String> {
    match args.scenario {
        Scenario::Store => judge(seed, args, &mut totals.totals),
        Scenario::Agents => judge_agents(seed, args, &mut totals.agent_totals),
        Scenario::Turns => judge_turns(seed, args, &mut totals.turn_totals, caught),
        Scenario::Runtime => judge_whole(seed, args, &mut totals.whole_totals),
        Scenario::Websocket => websocket::judge(
            seed,
            args.websocket,
            args.twice,
            args.options.verbose,
            &mut totals.websocket_totals,
        ),
        Scenario::HttpServer => httpserver::judge(
            seed,
            args.http_server,
            args.twice,
            args.options.verbose,
            &mut totals.http_server_totals,
        ),
        Scenario::Mpp => mpp::judge_seed(
            seed,
            args.mpp,
            args.twice,
            args.options.verbose,
            &mut totals.mpp_totals,
        ),
        Scenario::Sync => sync::judge_seed(
            seed,
            args.sync,
            args.twice,
            args.options.verbose,
            &mut totals.sync_totals,
        ),
        Scenario::CellCalls => cellcalls::judge(
            seed,
            args.cell_calls,
            args.twice,
            args.options.verbose,
            &mut totals.cell_calls_totals,
        ),
        Scenario::Live => live::judge(
            seed,
            args.live,
            args.twice,
            args.options.verbose,
            &mut totals.live_totals,
        ),
        Scenario::LongThread => longthread::judge(
            seed,
            args.long_thread,
            args.twice,
            args.options.verbose,
            &mut totals.long_thread_totals,
        ),
        Scenario::RequestClose => requestclose::judge(
            seed,
            args.twice,
            args.options.verbose,
            &mut totals.request_close_totals,
        ),
        Scenario::SteerCarry => steercarry::judge(
            seed,
            args.steer_carry,
            args.twice,
            args.options.verbose,
            &mut totals.steer_carry_totals,
        ),
        Scenario::AntigravityDurable => antigravitydurable::judge(
            seed,
            args.antigravity_durable,
            args.twice,
            args.options.verbose,
            &mut totals.antigravity_durable_totals,
        ),
        Scenario::InboxStress => inboxstress::judge(
            seed,
            args.inbox_stress,
            args.twice,
            args.options.verbose,
            &mut totals.inbox_stress_totals,
        ),
        Scenario::Hands => hands::judge_seed(
            seed,
            args.hands,
            hands::HandsOptions {
                fleet: args.fleet,
                reopen: args.reopen,
                cancel: args.cancel,
            },
            args.twice,
            args.options.verbose,
            &mut totals.hands_totals,
        ),
        Scenario::Streams => streams::judge(
            seed,
            args.streams,
            args.twice,
            args.options.verbose,
            &mut totals.streams_totals,
        ),
        Scenario::Executor => tasks::judge(
            seed,
            args.tasks,
            args.twice,
            args.options.verbose,
            &mut totals.tasks_totals,
        ),
        Scenario::Board => board::judge(
            seed,
            args.board,
            args.twice,
            args.options.verbose,
            &mut totals.board_totals,
        ),
        Scenario::Controlplane => controlplane::judge(
            seed,
            controlplane_agents(args),
            args.controlplane,
            args.twice,
            args.options.verbose,
            &mut totals.controlplane_totals,
        ),
        Scenario::Cost => cost::judge(
            seed,
            args.cost,
            args.twice,
            args.options.verbose,
            &mut totals.cost_totals,
        ),
        Scenario::Plog => plog::judge(
            seed,
            args.plog,
            args.twice,
            args.options.verbose,
            &mut totals.plog_totals,
        ),
        Scenario::Secrets => secrets::judge(
            seed,
            args.secrets,
            args.twice,
            args.options.verbose,
            &mut totals.secrets_totals,
        ),
        Scenario::Wakes => wakes::judge(
            seed,
            args.wakes,
            args.twice,
            args.options.verbose,
            &mut totals.wakes_totals,
        ),
        Scenario::Facts => facts::judge(seed, args.facts, args.twice, args.options.verbose),
        Scenario::BackendFacts => backendfacts::judge(seed, args.backend_facts, args.twice),
        Scenario::Imports => imports::judge(
            seed,
            args.imports,
            args.twice,
            args.options.verbose,
            &mut totals.imports_totals,
        ),
        Scenario::BoardShards => board_shards::judge(
            seed,
            args.board_shards,
            args.twice,
            args.options.verbose,
            &mut totals.shards_totals,
        ),
        Scenario::HandRouter => handrouter::judge(
            seed,
            args.hand_router,
            args.twice,
            args.options.verbose,
            &mut totals.hand_router_totals,
        ),
        Scenario::WorkspaceHost => workspacehost::judge(
            seed,
            args.workspace_host,
            args.twice,
            args.options.verbose,
            &mut totals.workspace_host_totals,
        ),
        Scenario::BoardSync => board_sync::judge(
            seed,
            args.board_sync,
            args.twice,
            args.options.verbose,
            &mut totals.board_sync_totals,
        ),
        Scenario::Dispatcher => dispatcher::judge(
            seed,
            args.dispatcher,
            args.twice,
            args.options.verbose,
            &mut totals.dispatcher_totals,
        ),
        Scenario::BoardSteer => board_steer::judge(
            seed,
            args.board_steer,
            args.twice,
            args.options.verbose,
            &mut totals.board_steer_totals,
        ),
        Scenario::BoardBrief => board_brief::judge(
            seed,
            args.board_brief,
            args.twice,
            args.options.verbose,
            &mut totals.board_brief_totals,
        ),
        Scenario::WorkspaceFs => workspacefs::judge(
            seed,
            args.workspace_fs,
            args.twice,
            args.options.verbose,
            &mut totals.workspace_fs_totals,
        ),
        Scenario::RepoCache => repocache::judge(
            seed,
            args.repo_cache,
            args.twice,
            args.options.verbose,
            &mut totals.repo_cache_totals,
        ),
        Scenario::WorkspaceSql => workspacesql::judge(
            seed,
            args.workspace_sql,
            args.twice,
            args.options.verbose,
            &mut totals.workspace_sql_totals,
        ),
        Scenario::GitRemote => gitremote::judge(
            seed,
            args.git_remote,
            args.twice,
            args.options.verbose,
            &mut totals.git_remote_totals,
        ),
        Scenario::WorkspaceStack => workspacestack::judge(
            seed,
            args.workspace_stack,
            args.twice,
            args.options.verbose,
            &mut totals.workspace_stack_totals,
        ),
        Scenario::WorkspaceCrash => workspacecrash::judge(
            seed,
            args.workspace_crash,
            args.twice,
            args.options.verbose,
            &mut totals.workspace_crash_totals,
        ),
        Scenario::Partitions => partitions::judge(
            seed,
            args.partitions,
            args.twice,
            args.options.verbose,
            &mut totals.partitions_totals,
        ),
        Scenario::Lease => lease::judge(
            seed,
            args.lease,
            args.twice,
            args.options.verbose,
            &mut totals.lease_totals,
        ),
        Scenario::Durable => durable::judge(
            seed,
            args.durable,
            args.twice,
            args.options.verbose,
            &mut totals.durable_totals,
        ),
        Scenario::Scale => scale::judge(
            seed,
            &args.scale,
            args.twice,
            args.options.verbose,
            &mut totals.scale_totals,
        ),
        Scenario::HandAuto => handauto::judge(
            seed,
            args.hand_auto,
            args.twice,
            args.options.verbose,
            &mut totals.hand_auto_totals,
        ),
        Scenario::NestedMeta => nestedmeta::judge(
            seed,
            args.nested_meta,
            args.twice,
            args.options.verbose,
            &mut totals.nested_meta_totals,
        ),
        Scenario::KvLease => kvlease::judge(
            seed,
            args.kv_lease,
            args.twice,
            args.options.verbose,
            &mut totals.kv_lease_totals,
        ),
        Scenario::Scope => scope::judge(
            seed,
            args.scope,
            args.twice,
            args.options.verbose,
            &mut totals.scope_totals,
        ),
        Scenario::ThreadScope => threadscope::judge(
            seed,
            args.thread_scope,
            args.twice,
            args.options.verbose,
            &mut totals.thread_scope_totals,
        ),
        Scenario::Migrated => migrated::judge(
            seed,
            args.migrated,
            args.twice,
            args.options.verbose,
            &mut totals.migrated_totals,
        ),
        Scenario::GcUnwind => gcunwind::judge(
            seed,
            args.gc_unwind,
            args.twice,
            args.options.verbose,
            &mut totals.gc_unwind_totals,
        ),
        Scenario::InterpDrop => interpdrop::judge(
            seed,
            args.interp_drop,
            args.twice,
            args.options.verbose,
            &mut totals.interp_drop_totals,
        ),
        Scenario::HeldAwait => heldawait::judge(
            seed,
            args.held_await,
            args.twice,
            args.options.verbose,
            &mut totals.held_await_totals,
        ),
        Scenario::Holds => holds::judge(
            seed,
            args.holds,
            args.twice,
            args.options.verbose,
            &mut totals.holds_totals,
        ),
        Scenario::PgPool => pgpool::judge(
            seed,
            args.pg_pool,
            args.twice,
            args.options.verbose,
            &mut totals.pg_pool_totals,
        ),
        Scenario::Slog => slog::judge(
            seed,
            args.slog,
            args.twice,
            args.options.verbose,
            &mut totals.slog_totals,
        ),
        Scenario::Append => append::judge(
            seed,
            args.append,
            args.twice,
            args.options.verbose,
            &mut totals.append_totals,
        ),
        Scenario::OneLogFacts => onelogfacts::judge(
            seed,
            args.one_log_facts,
            args.twice,
            args.options.verbose,
            &mut totals.one_log_facts_totals,
        ),
        #[cfg(unix)]
        Scenario::Capture => capture::judge(
            seed,
            args.capture,
            args.twice,
            args.options.verbose,
            &mut totals.capture_totals,
        ),
        #[cfg(not(unix))]
        Scenario::Capture => unreachable!("the capture scenario parses only on unix"),
        #[cfg(target_os = "linux")]
        Scenario::Dns => dns::judge(
            seed,
            args.dns,
            args.twice,
            args.options.verbose,
            &mut totals.dns_totals,
        ),
        #[cfg(not(target_os = "linux"))]
        Scenario::Dns => unreachable!("the dns scenario parses only on linux"),
        #[cfg(unix)]
        Scenario::Alloc => alloc::judge(
            seed,
            args.options.ops,
            args.alloc,
            args.twice,
            args.options.verbose,
            &mut totals.alloc_totals,
        ),
        #[cfg(not(unix))]
        Scenario::Alloc => unreachable!("the alloc scenario parses only on unix"),
        #[cfg(unix)]
        Scenario::Egress => egress::judge_seed(
            seed,
            args.egress,
            args.twice,
            args.options.verbose,
            &mut totals.egress_totals,
        ),
        #[cfg(not(unix))]
        Scenario::Egress => unreachable!("the egress scenario parses only on unix"),
    }
}

const WORKER_STACK_BYTES: usize = 64 << 20;

/// Runs every seed and hands each verdict to settle in seed order. With more than one job,
/// scoped workers pull seeds from a shared counter and judge them into their own totals; the
/// calling thread settles them in order and merges their totals, so the output and the totals
/// equal a run with one job.
fn each_seed(
    args: &Args,
    jobs: usize,
    totals: &mut Totals,
    caught: &mut Caught,
    mut settle: impl FnMut(u64, Option<String>),
) {
    if jobs <= 1 {
        for seed in args.seeds.clone() {
            let verdict = judge_one(seed, args, totals, caught);
            settle(seed, verdict);
        }
        return;
    }
    let seeds: Vec<u64> = args.seeds.clone().collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        for worker in 0..jobs.min(seeds.len()) {
            let sender = sender.clone();
            let (next, seeds) = (&next, &seeds);
            std::thread::Builder::new()
                .name(format!("seed-worker-{worker}"))
                .stack_size(WORKER_STACK_BYTES)
                .spawn_scoped(scope, move || {
                    loop {
                        let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some(&seed) = seeds.get(index) else {
                            break;
                        };
                        let mut own = Box::<Totals>::default();
                        let mut own_caught = Caught::new();
                        let verdict = judge_one(seed, args, &mut own, &mut own_caught);
                        if sender.send((index, verdict, own, own_caught)).is_err() {
                            break;
                        }
                    }
                })
                .expect("the OS starts a seed worker thread");
        }
        drop(sender);
        let mut waiting = BTreeMap::new();
        let mut settled = 0_usize;
        for (index, verdict, own, own_caught) in receiver {
            waiting.insert(index, (verdict, own, own_caught));
            while let Some((verdict, own, own_caught)) = waiting.remove(&settled) {
                totals.add(&own);
                merge_caught(caught, &own_caught);
                settle(seeds[settled], verdict);
                settled += 1;
            }
        }
    });
}

#[global_allocator]
static HEAP: memory::Heap = nanocode_sys::alloc::Counted(std::alloc::System);

fn main() -> ExitCode {
    if let Some(at) = std::env::args().position(|arg| arg == "--board-shards-bench") {
        let numbers: Vec<u64> = std::env::args()
            .skip(at + 1)
            .map_while(|arg| arg.parse().ok())
            .collect();
        let cost = board_shards::Cost {
            rtt_us: numbers.first().copied().unwrap_or(1_000),
            row_us: numbers.get(1).copied().unwrap_or(10),
            jitter_ms: 0,
        };
        let batch = numbers.get(2).copied().unwrap_or(256) as usize;
        let partitions = numbers
            .get(3)
            .map_or_else(|| vec![1, 4, 16, 64], |count| vec![*count as u32]);
        let writers = numbers
            .get(4)
            .map_or_else(|| vec![16, 256, 1_024], |count| vec![*count]);
        board_shards::bench(&partitions, &writers, batch, cost);
        return ExitCode::SUCCESS;
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--tidy"] {
        return tidy();
    }
    if args == ["--mutations"] {
        return mutations();
    }
    if args.first().map(String::as_str) == Some("--repo-cache-disk") {
        return repocache::disk(&args[1..]);
    }
    if args.first().is_some_and(|flag| flag == "--machines") {
        return machines::main(&args);
    }
    log::install();
    if let Err(error) = host::install() {
        eprintln!("could not install the simulated platform host: {error}");
        return ExitCode::FAILURE;
    }
    if args.first().map(String::as_str) == Some("--memory") {
        return memory::main(&args[1..]);
    }
    let Some(args) = parse_args(&args) else {
        return usage();
    };
    let mutation = args.options.mutation;
    let experiment = (args.scenario == Scenario::Turns
        && (args.turns.mutation != TurnMutation::None
            || args.turns.protections != Protections::ALL))
        || (args.scenario == Scenario::Runtime
            && (args.whole.mutation != WireMutation::None
                || args.whole.child_mutation != ChildMutation::None
                || args.whole.store_mutation != Mutation::None))
        || (args.scenario == Scenario::Agents && args.agents != AgentsMutation::None)
        || (args.scenario == Scenario::Websocket && args.websocket != WebSocketMutation::None)
        || (args.scenario == Scenario::HttpServer && args.http_server != HttpServerMutation::None)
        || (args.scenario == Scenario::Mpp && args.mpp != MppMutation::None)
        || (args.scenario == Scenario::Sync && args.sync != SyncMutation::None)
        || (args.scenario == Scenario::Streams && args.streams != StreamsMutation::None)
        || (args.scenario == Scenario::Hands && args.hands != HandsMutation::None)
        || (args.scenario == Scenario::CellCalls && args.cell_calls != CellCallsMutation::None)
        || (args.scenario == Scenario::Live && args.live != LiveMutation::None)
        || (args.scenario == Scenario::LongThread && args.long_thread != LongThreadMutation::None)
        || (args.scenario == Scenario::Executor && args.tasks != TasksMutation::None)
        || (args.scenario == Scenario::Board && args.board != BoardMutation::None)
        || (args.scenario == Scenario::Controlplane
            && args.controlplane != ControlplaneMutation::None)
        || (args.scenario == Scenario::Cost && args.cost != CostMutation::None)
        || (args.scenario == Scenario::Plog && args.plog != PlogMutation::None)
        || (args.scenario == Scenario::Secrets && args.secrets != SecretsMutation::None)
        || (args.scenario == Scenario::Wakes && args.wakes != WakesMutation::None)
        || (args.scenario == Scenario::Imports && args.imports != ImportsMutation::None)
        || (args.scenario == Scenario::Facts && args.facts != FactsMutation::None)
        || (args.scenario == Scenario::BackendFacts
            && args.backend_facts != BackendFactsMutation::None)
        || (args.scenario == Scenario::BoardShards && args.board_shards != ShardsMutation::None)
        || (args.scenario == Scenario::HandRouter && args.hand_router != HandRouterMutation::None)
        || (args.scenario == Scenario::Partitions && args.partitions != PartitionsMutation::None)
        || (args.scenario == Scenario::Lease && args.lease != LeaseMutation::None)
        || (args.scenario == Scenario::Durable && args.durable != DurableMutation::None)
        || (args.scenario == Scenario::Scale && args.scale.mutation != ScaleMutation::None)
        || (args.scenario == Scenario::WorkspaceHost
            && args.workspace_host != WorkspaceHostMutation::None)
        || (args.scenario == Scenario::BoardSync && args.board_sync != BoardSyncMutation::None)
        || (args.scenario == Scenario::Dispatcher && args.dispatcher != DispatcherMutation::NONE)
        || (args.scenario == Scenario::BoardSteer && args.board_steer != BoardSteerMutation::None)
        || (args.scenario == Scenario::SteerCarry && args.steer_carry != CarryMutation::None)
        || (args.scenario == Scenario::AntigravityDurable
            && args.antigravity_durable != AntigravityMutation::None)
        || (args.scenario == Scenario::InboxStress
            && args.inbox_stress != InboxStressMutation::None)
        || (args.scenario == Scenario::BoardBrief && args.board_brief != BoardBriefMutation::None)
        || (args.scenario == Scenario::WorkspaceFs
            && args.workspace_fs != WorkspaceFsMutation::None)
        || (args.scenario == Scenario::RepoCache && args.repo_cache != RepoCacheMutation::None)
        || (args.scenario == Scenario::WorkspaceSql
            && args.workspace_sql != WorkspaceSqlMutation::None)
        || (args.scenario == Scenario::GitRemote && args.git_remote != GitRemoteMutation::None)
        || (args.scenario == Scenario::WorkspaceStack
            && args.workspace_stack != WorkspaceStackMutation::None)
        || (args.scenario == Scenario::WorkspaceCrash
            && args.workspace_crash != WorkspaceCrashMutation::None)
        || (args.scenario == Scenario::HandAuto && args.hand_auto != HandAutoMutation::None)
        || (args.scenario == Scenario::NestedMeta && args.nested_meta != NestedMetaMutation::None)
        || (args.scenario == Scenario::GcUnwind && args.gc_unwind != GcUnwindMutation::None)
        || (args.scenario == Scenario::InterpDrop && args.interp_drop != InterpDropMutation::None)
        || (args.scenario == Scenario::HeldAwait && args.held_await != HeldAwaitMutation::None)
        || (args.scenario == Scenario::Holds && args.holds != HoldsMutation::None)
        || (args.scenario == Scenario::Migrated && args.migrated != MigratedMutation::None)
        || (args.scenario == Scenario::KvLease && args.kv_lease != KvLeaseMutation::None)
        || (args.scenario == Scenario::Scope && args.scope != ScopeMutation::NONE)
        || (args.scenario == Scenario::ThreadScope
            && args.thread_scope != ThreadScopeMutation::NONE)
        || (args.scenario == Scenario::PgPool && args.pg_pool != PgPoolMutation::None)
        || (args.scenario == Scenario::Slog && args.slog != SlogMutation::None)
        || (args.scenario == Scenario::Append && args.append != AppendMutation::None)
        || (args.scenario == Scenario::OneLogFacts
            && args.one_log_facts != OneLogFactsMutation::None)
        || alloc_experiment(&args);
    let (mut passed, mut failed) = (0_u64, 0_u64);
    let mut all = Totals::default();
    let mut caught = Caught::new();
    let jobs = if mutation != Mutation::None
        || experiment
        || matches!(
            args.scenario,
            Scenario::Executor
                | Scenario::Alloc
                | Scenario::Runtime
                | Scenario::Controlplane
                | Scenario::GcUnwind
                | Scenario::InterpDrop
        ) {
        // Mutations flip process-wide switches, the executor and alloc scenarios measure the
        // process itself, and runtime drives the whole runtime, whose ID counters are
        // process-wide and shift its schedules when seeds share a process, so those runs keep
        // to one seed at a time. Controlplane prints a SCORE line with wall-clock rates from
        // inside each seed, so it keeps one too. Python's collector is process-wide and a
        // collection skips while another holds it, so gc_unwind and interp_drop keep one too.
        1
    } else {
        args.jobs
    };
    each_seed(&args, jobs, &mut all, &mut caught, |seed, verdict| {
        if args.seed_lines {
            println!(
                "SEED seed={seed} {}",
                if verdict.is_some() { "fail" } else { "pass" }
            );
        }
        let Some(message) = verdict else {
            passed += 1;
            return;
        };
        failed += 1;
        let mut replay = replay(seed, &args);
        if args.twice && !replay.contains("--twice") {
            replay.push_str(" --twice");
        }
        if mutation == Mutation::None && !experiment {
            println!("FAIL seed={seed} Wrong {message} :: replay: {replay}");
        } else if failed <= u64::try_from(CAUGHT_SHOWN_MAX).expect("a small count fits in u64") {
            println!("CAUGHT seed={seed} {message} :: replay: {replay}");
        }
    });
    let Totals {
        totals,
        agent_totals,
        turn_totals,
        whole_totals,
        websocket_totals,
        http_server_totals,
        mpp_totals,
        sync_totals,
        streams_totals,
        hands_totals,
        cell_calls_totals,
        live_totals,
        long_thread_totals,
        request_close_totals,
        steer_carry_totals,
        antigravity_durable_totals,
        inbox_stress_totals,
        tasks_totals,
        board_totals,
        controlplane_totals,
        cost_totals,
        plog_totals,
        secrets_totals,
        wakes_totals,
        imports_totals,
        shards_totals,
        hand_router_totals,
        workspace_host_totals,
        board_sync_totals,
        dispatcher_totals,
        board_steer_totals,
        board_brief_totals,
        workspace_fs_totals,
        repo_cache_totals,
        workspace_sql_totals,
        git_remote_totals,
        workspace_stack_totals,
        workspace_crash_totals,
        partitions_totals,
        lease_totals,
        durable_totals,
        scale_totals,
        hand_auto_totals,
        nested_meta_totals,
        gc_unwind_totals,
        interp_drop_totals,
        held_await_totals,
        holds_totals,
        slog_totals,
        append_totals,
        one_log_facts_totals,
        migrated_totals,
        kv_lease_totals,
        scope_totals,
        thread_scope_totals,
        pg_pool_totals,
        #[cfg(unix)]
        capture_totals,
        #[cfg(target_os = "linux")]
        dns_totals,
        #[cfg(unix)]
        alloc_totals,
        #[cfg(unix)]
        egress_totals,
    } = all;
    if args.scenario == Scenario::Websocket {
        return websocket::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.websocket,
            &websocket_totals,
        );
    }
    if args.scenario == Scenario::Streams {
        return streams::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.streams,
            &streams_totals,
        );
    }
    if args.scenario == Scenario::Hands {
        return hands::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.hands,
            hands::HandsOptions {
                fleet: args.fleet,
                reopen: args.reopen,
                cancel: args.cancel,
            },
            &hands_totals,
        );
    }
    boardagents::report();
    if args.scenario == Scenario::CellCalls {
        return cellcalls::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.cell_calls,
            &cell_calls_totals,
        );
    }
    if args.scenario == Scenario::Live {
        return live::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.live,
            &live_totals,
        );
    }
    if args.scenario == Scenario::LongThread {
        return longthread::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.long_thread,
            &long_thread_totals,
        );
    }
    if args.scenario == Scenario::RequestClose {
        return requestclose::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            &request_close_totals,
        );
    }
    if args.scenario == Scenario::SteerCarry {
        return steercarry::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.steer_carry,
            &steer_carry_totals,
        );
    }
    if args.scenario == Scenario::AntigravityDurable {
        return antigravitydurable::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.antigravity_durable,
            &antigravity_durable_totals,
        );
    }
    if args.scenario == Scenario::InboxStress {
        return inboxstress::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.inbox_stress,
            &inbox_stress_totals,
        );
    }
    if args.scenario == Scenario::Board {
        return board::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.board,
            &board_totals,
        );
    }
    if args.scenario == Scenario::Plog {
        return plog::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.plog,
            &plog_totals,
        );
    }
    if args.scenario == Scenario::Secrets {
        return secrets::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.secrets,
            &secrets_totals,
        );
    }
    if args.scenario == Scenario::Wakes {
        return wakes::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.wakes,
            &wakes_totals,
        );
    }
    if args.scenario == Scenario::Facts {
        return facts::report(&args.seeds, passed, failed, args.twice, args.facts);
    }
    if args.scenario == Scenario::BackendFacts {
        return backendfacts::report(&args.seeds, passed, failed, args.twice, args.backend_facts);
    }
    if args.scenario == Scenario::Imports {
        return imports::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.imports,
            &imports_totals,
        );
    }
    if args.scenario == Scenario::BoardShards {
        return board_shards::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.board_shards,
            &shards_totals,
        );
    }
    if args.scenario == Scenario::Controlplane {
        return controlplane::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.controlplane,
            &controlplane_totals,
        );
    }
    if args.scenario == Scenario::Cost {
        return cost::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.cost,
            &cost_totals,
        );
    }
    if args.scenario == Scenario::HandRouter {
        return handrouter::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.hand_router,
            &hand_router_totals,
        );
    }
    if args.scenario == Scenario::WorkspaceSql {
        return workspacesql::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.workspace_sql,
            &workspace_sql_totals,
        );
    }
    if args.scenario == Scenario::WorkspaceFs {
        return workspacefs::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.workspace_fs,
            &workspace_fs_totals,
        );
    }
    if args.scenario == Scenario::RepoCache {
        return repocache::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.repo_cache,
            &repo_cache_totals,
        );
    }
    if args.scenario == Scenario::WorkspaceHost {
        return workspacehost::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.workspace_host,
            &workspace_host_totals,
        );
    }
    if args.scenario == Scenario::BoardSteer {
        return board_steer::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.board_steer,
            &board_steer_totals,
        );
    }
    if args.scenario == Scenario::BoardBrief {
        return board_brief::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.board_brief,
            &board_brief_totals,
        );
    }
    if args.scenario == Scenario::Dispatcher {
        return dispatcher::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.dispatcher,
            &dispatcher_totals,
        );
    }
    if args.scenario == Scenario::BoardSync {
        return board_sync::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.board_sync,
            &board_sync_totals,
        );
    }
    if args.scenario == Scenario::GitRemote {
        return gitremote::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.git_remote,
            &git_remote_totals,
        );
    }
    if args.scenario == Scenario::WorkspaceStack {
        return workspacestack::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.workspace_stack,
            &workspace_stack_totals,
        );
    }
    if args.scenario == Scenario::WorkspaceCrash {
        return workspacecrash::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.workspace_crash,
            &workspace_crash_totals,
        );
    }
    if args.scenario == Scenario::Scale {
        return scale::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            &args.scale,
            &scale_totals,
        );
    }
    if args.scenario == Scenario::Partitions {
        return partitions::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.partitions,
            &partitions_totals,
        );
    }
    if args.scenario == Scenario::Lease {
        return lease::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.lease,
            &lease_totals,
        );
    }
    if args.scenario == Scenario::Durable {
        return durable::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.durable,
            &durable_totals,
        );
    }
    if args.scenario == Scenario::HandAuto {
        return handauto::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.hand_auto,
            &hand_auto_totals,
        );
    }
    if args.scenario == Scenario::NestedMeta {
        return nestedmeta::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.nested_meta,
            &nested_meta_totals,
        );
    }
    if args.scenario == Scenario::Scope {
        return scope::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.scope,
            &scope_totals,
        );
    }
    if args.scenario == Scenario::ThreadScope {
        return threadscope::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.thread_scope,
            &thread_scope_totals,
        );
    }
    if args.scenario == Scenario::KvLease {
        return kvlease::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.kv_lease,
            &kv_lease_totals,
        );
    }
    if args.scenario == Scenario::Migrated {
        return migrated::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.migrated,
            &migrated_totals,
        );
    }
    if args.scenario == Scenario::GcUnwind {
        return gcunwind::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.gc_unwind,
            &gc_unwind_totals,
        );
    }
    if args.scenario == Scenario::InterpDrop {
        return interpdrop::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.interp_drop,
            &interp_drop_totals,
        );
    }
    if args.scenario == Scenario::HeldAwait {
        return heldawait::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.held_await,
            &held_await_totals,
        );
    }
    if args.scenario == Scenario::OneLogFacts {
        return onelogfacts::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.one_log_facts,
            &one_log_facts_totals,
        );
    }
    if args.scenario == Scenario::Slog {
        return slog::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.slog,
            &slog_totals,
        );
    }
    if args.scenario == Scenario::Append {
        return append::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.append,
            &append_totals,
        );
    }
    if args.scenario == Scenario::Holds {
        return holds::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.holds,
            &holds_totals,
        );
    }
    if args.scenario == Scenario::PgPool {
        return pgpool::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.pg_pool,
            &pg_pool_totals,
        );
    }
    if args.scenario == Scenario::Executor {
        return tasks::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.tasks,
            &tasks_totals,
        );
    }
    #[cfg(target_os = "linux")]
    if args.scenario == Scenario::Dns {
        return dns::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.dns,
            &dns_totals,
        );
    }
    #[cfg(unix)]
    if args.scenario == Scenario::Capture {
        return capture::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.capture,
            &capture_totals,
        );
    }
    if args.scenario == Scenario::Sync {
        return sync::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.sync,
            &sync_totals,
        );
    }
    if args.scenario == Scenario::Mpp {
        return mpp::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.mpp,
            &mpp_totals,
        );
    }
    if args.scenario == Scenario::HttpServer {
        return httpserver::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.http_server,
            &http_server_totals,
        );
    }
    #[cfg(unix)]
    if args.scenario == Scenario::Alloc {
        return alloc::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.alloc,
            &alloc_totals,
        );
    }
    #[cfg(unix)]
    if args.scenario == Scenario::Egress {
        return egress::report(
            &args.seeds,
            passed,
            failed,
            args.twice,
            args.egress,
            &egress_totals,
        );
    }
    if args.scenario == Scenario::Turns {
        return report_turns(&args, passed, failed, &turn_totals, &caught);
    }
    if args.scenario == Scenario::Runtime {
        println!(
            "RESULT seeds={}..{} scenario=runtime wire={} children={} mutation={} passed={passed} failed={failed} twice={} stats={whole_totals:?}",
            args.seeds.start,
            args.seeds.end,
            args.whole.wire.name(),
            args.whole.children.name(),
            mutation_name(&args.whole),
            args.twice
        );
        if let Some(name) = boardagents::mutation() {
            if failed > 0 {
                println!("MUTATION {name} caught by {failed} seeds");
                return ExitCode::SUCCESS;
            }
            println!("MUTATION {name} NOT caught");
            return ExitCode::FAILURE;
        }
        if args.whole.mutation != WireMutation::None
            || args.whole.child_mutation != ChildMutation::None
            || args.whole.store_mutation != Mutation::None
        {
            if failed > 0 {
                println!(
                    "MUTATION {} caught by {failed} seeds",
                    mutation_name(&args.whole)
                );
                return ExitCode::SUCCESS;
            }
            println!("MUTATION {} NOT caught", mutation_name(&args.whole));
            return ExitCode::FAILURE;
        }
        return if failed == 0 {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }
    if args.scenario == Scenario::Agents {
        println!(
            "RESULT seeds={}..{} scenario=agents mutation={} passed={passed} failed={failed} twice={} stats={agent_totals:?}",
            args.seeds.start,
            args.seeds.end,
            args.agents.name(),
            args.twice
        );
        if let Some(name) = boardagents::mutation() {
            if failed > 0 {
                println!("MUTATION {name} caught by {failed} seeds");
                return ExitCode::SUCCESS;
            }
            println!("MUTATION {name} NOT caught");
            return ExitCode::FAILURE;
        }
        if args.agents != AgentsMutation::None {
            if failed > 0 {
                println!("MUTATION {} caught by {failed} seeds", args.agents.name());
                return ExitCode::SUCCESS;
            }
            println!("MUTATION {} NOT caught", args.agents.name());
            return ExitCode::FAILURE;
        }
        return if failed == 0 {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }
    println!(
        "RESULT seeds={}..{} mutation={} passed={passed} failed={failed} twice={} stats={totals:?}",
        args.seeds.start,
        args.seeds.end,
        mutation.name(),
        args.twice
    );
    match mutation {
        Mutation::None if failed == 0 => ExitCode::SUCCESS,
        Mutation::None => ExitCode::FAILURE,
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
