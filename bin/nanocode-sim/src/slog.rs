use std::{
    collections::{BTreeMap, BTreeSet},
    future::{Future, poll_fn},
    ops::Range,
    pin::{Pin, pin},
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Poll,
    time::Duration,
};

use nanocode::{
    session_host::{LogEntry, SessionLog},
    turnlog::session_log_sim,
};
use nanocode_durability_core::{Sql, SqlRow, SqlStatement, StoreError, StoreFuture};
use nanocode_platform::HostFuture;
use nanocode_sync::{
    Atom, Slot, block_on,
    future::{Either, select},
    oneshot,
};
use serde_json::value::RawValue;

use crate::{
    executor::Clock,
    rng::{FNV_OFFSET, Rng, fnv},
};

const WRITERS_MIN: u64 = 2;
const WRITERS_MAX: u64 = 5;
const STEPS: Range<u64> = 200..1_200;
const CLAIM_PPM: u64 = 120_000;
const BEGIN_PPM: u64 = 600_000;
const STATEMENTS_MAX: u64 = 4;
const ABORT_PPM: u64 = 30_000;
const ACK_LOST_PPM: u64 = 50_000;
const ROUNDS: Range<u64> = 2..6;
const CALLS_MAX: u64 = 6;
const LINES_MAX: u64 = 3;
const REFUSE_PPM: u64 = 250_000;
const READ_PPM: u64 = 200_000;
const IN_FLIGHT_PPM: u64 = 500_000;
const FLUSH_PPM: u64 = 300_000;
const LANDS_PPM: u64 = 500_000;
const START_PPM: u64 = 600_000;
const SCOPE: &str = "slog";
const HEAD_CLAIM: &str = "INSERT INTO log_heads";
const HEAD_CAS: &str = "UPDATE log_heads SET version";
const READ_FORMAT: &str = "FROM nanocode_formats WHERE component";
const WRITE_FORMAT: &str = "INSERT INTO nanocode_formats";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlogMutation {
    None,
    AppendSkipsEpoch,
    WriterNeverRespawned,
    StagedDropped,
    HeldAfterJob,
    HeldUnwritten,
    StartWriterKeepsStopped,
}

impl SlogMutation {
    const ALL: [Self; 7] = [
        Self::None,
        Self::AppendSkipsEpoch,
        Self::WriterNeverRespawned,
        Self::StagedDropped,
        Self::HeldAfterJob,
        Self::HeldUnwritten,
        Self::StartWriterKeepsStopped,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|mutation| mutation.name() == name)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::AppendSkipsEpoch => "append_skips_epoch",
            Self::WriterNeverRespawned => "writer_never_respawned",
            Self::StagedDropped => "staged_dropped",
            Self::HeldAfterJob => "held_after_job",
            Self::HeldUnwritten => "held_unwritten",
            Self::StartWriterKeepsStopped => "start_writer_keeps_stopped",
        }
    }

    const fn session_log(self) -> session_log_sim::Mutation {
        match self {
            Self::None | Self::AppendSkipsEpoch => session_log_sim::Mutation::None,
            Self::WriterNeverRespawned => session_log_sim::Mutation::WriterNeverRespawned,
            Self::StagedDropped => session_log_sim::Mutation::StagedDropped,
            Self::HeldAfterJob => session_log_sim::Mutation::HeldAfterJob,
            Self::HeldUnwritten => session_log_sim::Mutation::HeldUnwritten,
            Self::StartWriterKeepsStopped => session_log_sim::Mutation::StartWriterKeepsStopped,
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
pub struct SlogStats {
    pub seeds: u64,
    pub claims: u64,
    pub commits: u64,
    pub landed: u64,
    pub lost: u64,
    pub waited: u64,
    pub aborts: u64,
    pub acks_lost: u64,
    pub writer_claims: u64,
    pub stops_idle: u64,
    pub stops_in_flight: u64,
    pub stalls_landed: u64,
    pub callers_dropped: u64,
    pub new_callers_first: u64,
    pub starts: u64,
    pub staged_reported: u64,
    pub lines_landed: u64,
}

impl SlogStats {
    pub const fn add(&mut self, other: &Self) {
        self.seeds += other.seeds;
        self.claims += other.claims;
        self.commits += other.commits;
        self.landed += other.landed;
        self.lost += other.lost;
        self.waited += other.waited;
        self.aborts += other.aborts;
        self.acks_lost += other.acks_lost;
        self.writer_claims += other.writer_claims;
        self.stops_idle += other.stops_idle;
        self.stops_in_flight += other.stops_in_flight;
        self.stalls_landed += other.stalls_landed;
        self.callers_dropped += other.callers_dropped;
        self.new_callers_first += other.new_callers_first;
        self.starts += other.starts;
        self.staged_reported += other.staged_reported;
        self.lines_landed += other.lines_landed;
    }
}

enum Head {
    Pending,
    Held,
    Missed,
}

struct Batch {
    epoch: u64,
    head: Head,
    statements: u64,
    staged: Vec<u64>,
}

#[derive(Default)]
struct Writer {
    epoch: Option<u64>,
    batch: Option<Batch>,
}

struct World {
    mutation: SlogMutation,
    epoch: u64,
    issued: BTreeSet<u64>,
    locked: Option<usize>,
    rows: Vec<(u64, usize, u64)>,
    next: u64,
    writers: Vec<Writer>,
    stats: SlogStats,
    wrong: Option<String>,
}

impl World {
    fn claim(&mut self, writer: usize) {
        if self.locked.is_some_and(|holder| holder != writer) {
            self.stats.waited += 1;
            return;
        }
        let before = self.epoch;
        self.epoch += 1;
        assert!(self.epoch > before);
        if !self.issued.insert(self.epoch) {
            self.wrong = Some(format!("epoch {} was issued twice", self.epoch));
        }
        self.writers[writer].epoch = Some(self.epoch);
        self.stats.claims += 1;
    }

    fn head(&mut self, writer: usize, epoch: u64) -> Option<Head> {
        if self.mutation == SlogMutation::AppendSkipsEpoch {
            return Some(Head::Held);
        }
        if self.locked.is_some_and(|holder| holder != writer) {
            self.stats.waited += 1;
            return None;
        }
        if self.epoch != epoch {
            return Some(Head::Missed);
        }
        self.locked = Some(writer);
        Some(Head::Held)
    }

    fn guarded(&self, epoch: u64) -> bool {
        self.mutation == SlogMutation::AppendSkipsEpoch || self.epoch == epoch
    }

    fn step(&mut self, writer: usize, rng: &mut Rng) {
        let Some(mut batch) = self.writers[writer].batch.take() else {
            match self.writers[writer].epoch {
                Some(epoch) if !rng.chance(CLAIM_PPM) => {
                    if rng.chance(BEGIN_PPM) {
                        self.writers[writer].batch = Some(Batch {
                            epoch,
                            head: Head::Pending,
                            statements: rng.range(1, STATEMENTS_MAX + 1),
                            staged: Vec::new(),
                        });
                    }
                }
                _ => self.claim(writer),
            }
            return;
        };
        if matches!(batch.head, Head::Pending) {
            if let Some(head) = self.head(writer, batch.epoch) {
                batch.head = head;
            }
            self.writers[writer].batch = Some(batch);
            return;
        }
        if batch.statements > 0 {
            batch.statements -= 1;
            if self.guarded(batch.epoch) {
                batch.staged.push(self.next);
                self.next += 1;
            }
            self.writers[writer].batch = Some(batch);
            return;
        }
        self.finish(writer, batch, rng);
    }

    fn finish(&mut self, writer: usize, batch: Batch, rng: &mut Rng) {
        if self.locked == Some(writer) {
            self.locked = None;
        }
        if rng.chance(ABORT_PPM) {
            self.stats.aborts += 1;
            return;
        }
        for seq in &batch.staged {
            if self.epoch != batch.epoch && self.wrong.is_none() {
                self.wrong = Some(format!(
                    "writer {writer} appended seq {seq} at epoch {} after the head moved to {}",
                    batch.epoch, self.epoch
                ));
            }
            self.rows.push((*seq, writer, batch.epoch));
            self.stats.landed += 1;
        }
        self.stats.commits += 1;
        match batch.head {
            Head::Missed => {
                if !batch.staged.is_empty() && self.wrong.is_none() {
                    self.wrong = Some(format!(
                        "writer {writer} at epoch {} was refused as Lost but {} rows landed",
                        batch.epoch,
                        batch.staged.len()
                    ));
                }
                self.writers[writer].epoch = None;
                self.stats.lost += 1;
            }
            Head::Held | Head::Pending => {
                if rng.chance(ACK_LOST_PPM) {
                    self.stats.acks_lost += 1;
                }
            }
        }
    }
}

struct Stall {
    reached: oneshot::Sender<()>,
    lands: bool,
}

#[derive(Default)]
struct Store {
    epoch: AtomicU64,
    claims: AtomicU64,
    refuse: AtomicBool,
    stall: Slot<Stall>,
    formats: Atom<BTreeMap<String, String>>,
    landed: Atom<String>,
}

impl Store {
    fn answer(&self, statement: &SqlStatement<'_>) -> Vec<SqlRow> {
        if let [component, version] = statement.params[..]
            && statement.sql.starts_with(WRITE_FORMAT)
        {
            self.formats
                .update(|formats| formats.insert(component.to_owned(), version.to_owned()));
            return Vec::new();
        }
        if let [component] = statement.params[..]
            && statement.sql.contains(READ_FORMAT)
        {
            return self.formats.read(|formats| {
                formats
                    .get(component)
                    .map(|version| vec![vec![Some(version.clone())]])
                    .unwrap_or_default()
            });
        }
        if statement.sql.starts_with(HEAD_CLAIM) {
            self.claims.fetch_add(1, Ordering::Relaxed);
            let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
            return vec![vec![Some(epoch.to_string())]];
        }
        if statement.sql.starts_with(HEAD_CAS) {
            let epoch = self.epoch.load(Ordering::Relaxed).to_string();
            if statement.params.get(1) == Some(&epoch.as_str()) {
                return vec![vec![Some(epoch)]];
            }
        }
        Vec::new()
    }

    fn land(&self, statements: &[SqlStatement<'_>]) {
        let params = statements
            .iter()
            .flat_map(|statement| statement.params.iter().copied())
            .collect::<String>();
        self.landed.update(|landed| landed.push_str(&params));
    }

    fn landed(&self, line: u64) -> bool {
        self.landed
            .read(|landed| landed.contains(&format!("{{\"line\":{line}}}")))
    }
}

impl Sql for Store {
    fn transaction<'a>(
        &'a self,
        _operation: &'static str,
        statements: &'a [SqlStatement<'a>],
    ) -> StoreFuture<'a, Result<Vec<Vec<SqlRow>>, StoreError>> {
        let write = statements
            .first()
            .is_some_and(|statement| statement.sql.starts_with(HEAD_CAS));
        if write && self.refuse.swap(false, Ordering::Relaxed) {
            return Box::pin(async {
                Err(StoreError::Backend(
                    "the simulated store refused the write".into(),
                ))
            });
        }
        if write && let Some(stall) = self.stall.take() {
            if stall.lands {
                self.land(statements);
            }
            stall.reached.send(()).ok();
            return Box::pin(std::future::pending());
        }
        let rows = statements
            .iter()
            .map(|statement| self.answer(statement))
            .collect();
        if write {
            self.land(statements);
        }
        Box::pin(async move { Ok(rows) })
    }

    fn call_bound(&self) -> Option<Duration> {
        None
    }
}

fn drive<T>(tasks: &mut Vec<HostFuture>, call: impl Future<Output = T>) -> T {
    let mut call = pin!(call);
    block_on(poll_fn(|context| {
        if let Poll::Ready(output) = call.as_mut().poll(context) {
            return Poll::Ready(output);
        }
        tasks.extend(crate::host::spawned().into_iter().map(|(task, _)| task));
        tasks.retain_mut(|task| task.as_mut().poll(context).is_pending());
        Poll::Pending
    }))
}

fn lines(rng: &mut Rng, next: &mut u64) -> (Vec<u64>, Vec<LogEntry>) {
    let marks = (0..rng.range(1, LINES_MAX + 1))
        .map(|_| {
            *next += 1;
            *next
        })
        .collect::<Vec<_>>();
    let entries = marks
        .iter()
        .map(|mark| LogEntry {
            request_id: None,
            event: RawValue::from_string(format!("{{\"line\":{mark}}}"))
                .expect("a simulated line is JSON"),
        })
        .collect();
    (marks, entries)
}

fn land(store: &Store, staged: &mut Vec<u64>, stats: &mut SlogStats) -> Result<(), String> {
    if let Some(line) = staged.iter().find(|line| !store.landed(**line)) {
        return Err(format!(
            "line {line} was staged and a later write succeeded, but it never landed"
        ));
    }
    stats.lines_landed += staged.len() as u64;
    staged.clear();
    Ok(())
}

fn in_flight(round: u64, outcome: Result<(), StoreError>) -> Result<StoreError, String> {
    match outcome {
        Ok(()) => Err(format!(
            "round {round}: a call in flight when its writer stopped reported success"
        )),
        Err(StoreError::NotCommitted(error)) => Err(format!(
            "round {round}: a call in flight when its writer stopped, whose write may have landed, reported it was not committed: {error}"
        )),
        Err(error) => Ok(error),
    }
}

fn unsent<T>(
    round: u64,
    store: &Store,
    marks: &[u64],
    outcome: Result<T, StoreError>,
) -> Result<StoreError, String> {
    match outcome {
        Ok(_) => Err(format!(
            "round {round}: the first call after its writer stopped with a write in flight ran without reporting that write"
        )),
        Err(error @ StoreError::NotCommitted(_))
            if marks.iter().all(|mark| !store.landed(*mark)) =>
        {
            Ok(error)
        }
        Err(error) => Err(format!(
            "round {round}: the first call after its writer stopped with a write in flight did not report that it never ran: {error}"
        )),
    }
}

fn held_reported(
    round: u64,
    reported: &StoreError,
    staged: &mut Vec<u64>,
    stats: &mut SlogStats,
) -> Result<(), String> {
    if !reported.to_string().contains(&format!(
        "holding {} events whose outcome is unknown",
        staged.len()
    )) {
        return Err(format!(
            "round {round}: the writer stopped holding {} lines, but the call reported: {reported}",
            staged.len()
        ));
    }
    stats.staged_reported += staged.len() as u64;
    staged.clear();
    Ok(())
}

fn stop_writers(rng: &mut Rng, stats: &mut SlogStats) -> Result<u64, String> {
    let store = Arc::new(Store::default());
    let migrated = block_on(SessionLog::migrate(&*store))
        .map_err(|error| format!("migrate the simulated session log: {error}"))?;
    let log = SessionLog::new(Arc::clone(&store), SCOPE, &migrated);
    let mut tasks = Vec::new();
    let mut next = 0;
    let mut staged = Vec::new();
    let mut never = Vec::new();
    let mut digest = FNV_OFFSET;
    for round in 0..rng.range(ROUNDS.start, ROUNDS.end) {
        for _ in 0..rng.range(1, CALLS_MAX + 1) {
            if rng.chance(READ_PPM) {
                drive(&mut tasks, log.turn("absent")).map_err(|error| {
                    format!("round {round}: a read with no fault failed: {error}")
                })?;
                continue;
            }
            let refused = rng.chance(REFUSE_PPM);
            store.refuse.store(refused, Ordering::Relaxed);
            let (marks, entries) = lines(rng, &mut next);
            staged.extend(marks);
            match drive(&mut tasks, log.stage(&entries)) {
                Ok(_) => land(&store, &mut staged, stats)
                    .map_err(|error| format!("round {round}: {error}"))?,
                Err(_) if refused => {}
                Err(error) => {
                    return Err(format!(
                        "round {round}: a stage with no fault failed: {error}"
                    ));
                }
            }
            digest = fnv(digest, &[u8::from(refused)]);
        }
        digest = fnv(digest, &(staged.len() as u64).to_le_bytes());
        store.refuse.store(false, Ordering::Relaxed);
        let (marks, entries) = lines(rng, &mut next);
        let reported = if rng.chance(IN_FLIGHT_PPM) {
            stats.stops_in_flight += 1;
            let flush = !staged.is_empty() && rng.chance(FLUSH_PPM);
            if !flush {
                staged.extend(marks);
            }
            let (reached, stalled) = oneshot::channel();
            let lands = rng.chance(LANDS_PPM);
            stats.stalls_landed += u64::from(lands);
            store.stall.put(Stall { reached, lands });
            let mut call: Pin<Box<dyn Future<Output = Result<(), StoreError>> + '_>> = if flush {
                Box::pin(log.flush())
            } else {
                Box::pin(async { log.stage(&entries).await.map(drop) })
            };
            if let Either::Left(_) = drive(&mut tasks, select(call.as_mut(), stalled)) {
                return Err(format!(
                    "round {round}: a call finished through a stalled write"
                ));
            }
            let (mut fresh, entries) = lines(rng, &mut next);
            let order = rng.range(0, 3);
            digest = fnv(digest, &[u8::from(flush), u8::from(lands), order as u8]);
            match order {
                0 => {
                    tasks.clear();
                    let reported = in_flight(round, drive(&mut tasks, call))?;
                    drive(&mut tasks, log.stage(&entries)).map_err(|error| {
                        format!("round {round}: the call after the one in flight when its writer stopped failed: {error}")
                    })?;
                    land(&store, &mut fresh, stats)
                        .map_err(|error| format!("round {round}: {error}"))?;
                    reported
                }
                1 => {
                    stats.new_callers_first += 1;
                    tasks.clear();
                    let reported = unsent(
                        round,
                        &store,
                        &fresh,
                        drive(&mut tasks, log.stage(&entries)),
                    )?;
                    in_flight(round, drive(&mut tasks, call))?;
                    never.extend(fresh);
                    reported
                }
                _ => {
                    stats.callers_dropped += 1;
                    drop(call);
                    tasks.clear();
                    let reported = unsent(
                        round,
                        &store,
                        &fresh,
                        drive(&mut tasks, log.stage(&entries)),
                    )?;
                    never.extend(fresh);
                    reported
                }
            }
        } else {
            stats.stops_idle += 1;
            tasks.clear();
            if rng.chance(START_PPM) {
                stats.starts += 1;
                match log.start_writer() {
                    Ok(()) => {
                        let started = crate::host::spawned();
                        if started.is_empty() {
                            return Err(format!(
                                "round {round}: a writer start after its writer stopped started no writer"
                            ));
                        }
                        tasks.extend(started.into_iter().map(|(task, _)| task));
                    }
                    Err(error) if staged.is_empty() => {
                        return Err(format!(
                            "round {round}: a writer start after its writer stopped idle failed: {error}"
                        ));
                    }
                    Err(error) => held_reported(round, &error, &mut staged, stats)?,
                }
            }
            match drive(&mut tasks, log.stage(&entries)) {
                Ok(_) => {
                    staged.extend(marks);
                    land(&store, &mut staged, stats).map_err(|error| {
                        format!("round {round}: after its writer stopped idle, {error}")
                    })?;
                    continue;
                }
                Err(error) if staged.is_empty() => {
                    return Err(format!(
                        "round {round}: the first call after its writer stopped idle failed: {error}"
                    ));
                }
                Err(error) => {
                    never.extend(marks);
                    error
                }
            }
        };
        held_reported(round, &reported, &mut staged, stats)?;
    }
    drive(&mut tasks, log.turn("absent")).map_err(|error| {
        format!("the session log stopped serving after its writer stopped: {error}")
    })?;
    if let Some(line) = never.iter().find(|line| store.landed(**line)) {
        return Err(format!(
            "line {line} landed, but its call reported that it never ran"
        ));
    }
    stats.writer_claims += store.claims.load(Ordering::Relaxed);
    Ok(fnv(
        digest,
        &store.epoch.load(Ordering::Relaxed).to_le_bytes(),
    ))
}

fn writer_stops(seed: u64, mutation: SlogMutation, stats: &mut SlogStats) -> Result<u64, String> {
    session_log_sim::mutate(mutation.session_log());
    let outcome = crate::host::entered(seed, &Clock::new(), || {
        stop_writers(&mut Rng::derive(seed, "slog_writers"), stats)
    });
    session_log_sim::mutate(session_log_sim::Mutation::None);
    outcome
}

struct Outcome {
    digest: u64,
    wrong: Option<String>,
    stats: SlogStats,
    shape: String,
}

fn run_seed(seed: u64, mutation: SlogMutation) -> Outcome {
    let mut rng = Rng::derive(seed, "slog");
    let writers = rng.range(WRITERS_MIN, WRITERS_MAX + 1);
    let steps = rng.range(STEPS.start, STEPS.end);
    let mut world = World {
        mutation,
        epoch: 0,
        issued: BTreeSet::new(),
        locked: None,
        rows: Vec::new(),
        next: 1,
        writers: (0..writers).map(|_| Writer::default()).collect(),
        stats: SlogStats {
            seeds: 1,
            ..SlogStats::default()
        },
        wrong: None,
    };
    for _ in 0..steps {
        let writer = rng.pick(world.writers.len());
        world.step(writer, &mut rng);
        if world.wrong.is_some() {
            break;
        }
    }
    let mut digest = FNV_OFFSET;
    for (seq, writer, epoch) in &world.rows {
        digest = fnv(digest, &seq.to_le_bytes());
        digest = fnv(digest, &writer.to_le_bytes());
        digest = fnv(digest, &epoch.to_le_bytes());
    }
    digest = fnv(digest, &world.epoch.to_le_bytes());
    if world.wrong.is_none() {
        match writer_stops(seed, mutation, &mut world.stats) {
            Ok(stops) => digest = fnv(digest, &stops.to_le_bytes()),
            Err(wrong) => world.wrong = Some(wrong),
        }
    }
    Outcome {
        digest,
        wrong: world.wrong,
        stats: world.stats,
        shape: format!("writers={writers} steps={steps}"),
    }
}

pub fn judge(
    seed: u64,
    mutation: SlogMutation,
    twice: bool,
    verbose: bool,
    totals: &mut SlogStats,
) -> Option<String> {
    let first = run_seed(seed, mutation);
    totals.add(&first.stats);
    if verbose {
        println!(
            "seed {seed} digest {:016x} {} stats={:?}",
            first.digest, first.shape, first.stats
        );
    }
    if first.wrong.is_some() || !twice {
        return first
            .wrong
            .map(|message| format!("{} :: {message}", first.shape));
    }
    let second = run_seed(seed, mutation);
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "{} :: nondeterminism: digest {:016x} then {:016x}",
            first.shape, first.digest, second.digest
        )
    })
}

pub fn replay(seed: u64, mutation: SlogMutation) -> String {
    let mut command = format!("nanocode-sim --scenario slog --seed {seed}");
    if mutation != SlogMutation::None {
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
    mutation: SlogMutation,
    totals: &SlogStats,
) -> ExitCode {
    println!(
        "RESULT seeds={}..{} scenario=slog mutation={} passed={passed} failed={failed} twice={twice} stats={totals:?}",
        seeds.start,
        seeds.end,
        mutation.name()
    );
    match mutation {
        SlogMutation::None if failed == 0 => ExitCode::SUCCESS,
        SlogMutation::None => ExitCode::FAILURE,
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
