#!/usr/bin/env node
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

process.removeAllListeners('warning');
process.on('warning', (warning) => {
  if (warning.name !== 'ExperimentalWarning') console.warn(warning);
});
const { DatabaseSync } = await import('node:sqlite');

const SCHEMA_VERSION = '2';
const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const CHUNK_BYTES = 64 << 20;
const INTEGER = /^(0|[1-9]\d{0,14})$/;
const COLUMN_ATTRIBUTES = new Set([
  'code.file.path', 'code.module.name', 'code.line.number', 'thread.id', 'thread.name', 'target',
  'busy_ns', 'idle_ns', 'session.id', 'tool.name', 'tool.call_id', 'model', 'status',
]);
const DEFAULT_GROUP = {
  'tool.execute': ['tool'],
  'tool.call': ['tool'],
  'attached_tool.execute': ['tool'],
  'nc.tool_call': ['tool'],
  'model.call': ['model'],
  'nc.model_call': ['model'],
  'durability.store': ['operation'],
  'nc.action': ['action.name'],
  'nc.command': ['command.name'],
  'nc.input': ['input.action'],
};
const COLUMNS = new Set([
  'name', 'tool', 'model', 'status', 'error', 'session_id', 'session_from', 'thread_id', 'thread_name', 'pid',
  'call_id', 'trace_id', 'kind',
]);
const UI_NAMES = new Set(['nc.frame', 'nc.view', 'nc.view.layout', 'nc.transcript.apply', 'nc.app_event', 'nc.input']);
const isUi = (name) => name.startsWith('tui.') || UI_NAMES.has(name) || (name.startsWith('nc.') && name.endsWith('.render'));

const HELP = `Query nanocode OpenTelemetry traces and metrics with SQL.

Usage: node scripts/otel.mjs <command> [arguments] [options]

Commands:
  ingest                 Load new OTLP JSONL lines from <home>/log/otel into the SQLite cache
  schema                 Show tables, views, span names with counts, and attribute keys
  sql <query>            Run SQL against the cache. Tables: spans, events, links, metrics, sessions, callsites,
                         sources. Views: tools, cells, models, turns, frames
  tails                  Latency percentiles and log-scale histograms per group
  outliers               Spans slower than their group's percentile threshold, with session, thread, process,
                         arguments and how many other processes had a span of that name over the threshold at
                         the same time (a machine-wide stall rather than this code path)
  trace <id>             Ancestors and descendant tree of a span (row id, span id or tool call id) with self time
  sessions               Sessions (threads) with turn, model and tool totals
  breakdown              Turn wall time split exclusively into model calls, tool work (exec cells until they finish,
                         waits, Codex tool calls and the terminal's tool calls), durable store writes and other
                         (the agent loop itself), per session (--by model or --by none to aggregate)
  builds                 Executables that produced telemetry (path and build time), with processes, sessions and spans
  prune                  Delete raw telemetry directories older than --older-than whose process has exited and
                         whose files are fully ingested; their spans stay in the cache (--dry-run lists them)
  report                 Write an HTML report with tail (CCDF) plots, latency-over-time scatter plots and outliers

Options:
  --home <dir>           Nanocode home (default $NANOCODE_HOME or ~/.nanocode)
  --db <file>            Cache database (default output/telemetry/telemetry.sqlite in this repository)
  --span <name>          Span name or SQL LIKE pattern such as 'tool.%' (default tool.execute)
  --by <keys>            Comma-separated group keys: span columns (tool, model, session_id, thread_name, status,
                         name, pid), build (executable and build time) or attribute keys such as tool.hand or
                         operation; 'none' disables grouping
  --since <age|time>     Only spans that started within 30m, 6h, 2d, or after an ISO timestamp
  --session <prefix>     Only spans whose session id starts with this prefix
  --where <sql>          Extra SQL condition on the spans table (alias s), e.g. "s.dur_ms > 1000"
  --above <percentile>   Outlier threshold percentile within each group (default 99)
  --min <count>          Minimum group size for percentile thresholds in outliers (default 20)
  --limit <n>            Row limit (default 25)
  --hist                 Print a full histogram for each group in tails
  --json                 Print JSON rows
  --full                 Do not shorten wide cells
  --no-ingest            Query the cache without loading new telemetry first
  --out <file>           Report path (default output/telemetry/report.html)
  --points <n>           Scatter points per report section (default 5000)
`;

function parseArgs(argv) {
  const options = { _: [] };
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (!arg.startsWith('--')) {
      options._.push(arg);
      continue;
    }
    const [key, inline] = arg.slice(2).split(/=(.*)/s, 2);
    if (['hist', 'json', 'full', 'no-ingest', 'help', 'dry-run'].includes(key)) {
      options[key] = true;
    } else if (inline !== undefined) {
      options[key] = inline;
    } else if (index + 1 < argv.length) {
      options[key] = argv[++index];
    } else {
      fail('missing value for --' + key);
    }
  }
  return options;
}

function fail(message) {
  console.error('otel: ' + message);
  process.exit(1);
}

function homeDir(options) {
  return path.resolve(options.home ?? process.env.NANOCODE_HOME ?? path.join(os.homedir(), '.nanocode'));
}

function openDb(file) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const db = new DatabaseSync(file);
  db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = OFF; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -262144;');
  const hasMeta = db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta'").get();
  const version = hasMeta ? db.prepare("SELECT value FROM meta WHERE key = 'schema'").get()?.value : undefined;
  if (version !== undefined && version !== SCHEMA_VERSION) {
    db.close();
    for (const suffix of ['', '-wal', '-shm']) fs.rmSync(file + suffix, { force: true });
    return openDb(file);
  }
  db.exec(`
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
    INSERT OR IGNORE INTO meta VALUES ('schema', '${SCHEMA_VERSION}');
    CREATE TABLE IF NOT EXISTS sources(
      id INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE, kind TEXT NOT NULL, pid INTEGER, started_us INTEGER,
      offset INTEGER NOT NULL DEFAULT 0, service_version TEXT);
    CREATE TABLE IF NOT EXISTS callsites(
      id INTEGER PRIMARY KEY, target TEXT, file TEXT, line INTEGER, module TEXT, UNIQUE(target, file, line, module));
    CREATE TABLE IF NOT EXISTS spans(
      id INTEGER PRIMARY KEY, source INTEGER NOT NULL, pid INTEGER, trace_id TEXT NOT NULL, span_id TEXT NOT NULL,
      parent_id TEXT, name TEXT NOT NULL, kind INTEGER, start_us INTEGER NOT NULL, end_us INTEGER NOT NULL,
      dur_ms REAL NOT NULL, span_ms REAL NOT NULL, busy_ms REAL, status TEXT, error INTEGER NOT NULL,
      thread_id INTEGER, thread_name TEXT, session_id TEXT, session_from TEXT, tool TEXT, call_id TEXT, model TEXT,
      callsite INTEGER, ui INTEGER NOT NULL, attrs TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS events(
      span INTEGER NOT NULL, time_us INTEGER, name TEXT, kind TEXT, content TEXT, attrs TEXT);
    CREATE TABLE IF NOT EXISTS links(span INTEGER NOT NULL, trace_id TEXT, span_id TEXT);
    CREATE TABLE IF NOT EXISTS metrics(
      source INTEGER NOT NULL, name TEXT NOT NULL, unit TEXT, type TEXT, time_us INTEGER, start_us INTEGER, attrs TEXT,
      value REAL, count INTEGER, sum REAL, min REAL, max REAL, bounds TEXT, counts TEXT);
    CREATE TABLE IF NOT EXISTS sessions(
      session_id TEXT PRIMARY KEY, root_session_id TEXT, parent_session_id TEXT, origin TEXT, depth INTEGER,
      model TEXT, effort TEXT, first_us INTEGER, last_us INTEGER, turns INTEGER, model_calls INTEGER,
      model_ms REAL, cells INTEGER, cell_ms REAL, tool_calls INTEGER, tool_ms REAL, spans INTEGER);
    DROP VIEW IF EXISTS tools;
    CREATE VIEW tools AS
      SELECT s.id, datetime(s.start_us / 1e6, 'unixepoch', 'localtime') AS at, s.start_us, s.tool, s.dur_ms,
        json_extract(s.attrs, '$."duration_ns"') / 1e6 AS exec_ms,
        json_extract(s.attrs, '$."tool.admission.wait_ns"') / 1e6 AS admission_ms,
        s.status, s.error, s.session_id, s.thread_id, s.thread_name, s.pid, s.call_id,
        json_extract(s.attrs, '$."tool.hand"') AS hand,
        json_extract(s.attrs, '$."process.exit.code"') AS exit_code,
        json_extract(s.attrs, '$."tool.arguments.bytes"') AS input_bytes,
        json_extract(s.attrs, '$."tool.output.bytes"') AS output_bytes,
        (SELECT e.content FROM events e WHERE e.span = s.id AND e.kind = 'tool.arguments' LIMIT 1) AS arguments,
        s.trace_id, s.span_id, s.parent_id
      FROM spans s WHERE s.name = 'tool.execute';
    DROP VIEW IF EXISTS cells;
    CREATE VIEW cells AS
      SELECT s.id, datetime(s.start_us / 1e6, 'unixepoch', 'localtime') AS at, s.start_us, s.name, s.dur_ms, s.status,
        s.error, s.session_id, s.call_id, json_extract(s.attrs, '$."cell.id"') AS cell_id,
        json_extract(s.attrs, '$.running') AS running, json_extract(s.attrs, '$."nested.count"') AS nested,
        json_extract(s.attrs, '$."source.bytes"') AS source_bytes,
        json_extract(s.attrs, '$."wait.yield_time_ms"') AS yield_time_ms, s.thread_name, s.trace_id, s.span_id
      FROM spans s WHERE s.name IN ('code_mode.cell', 'code_mode.wait');
    DROP VIEW IF EXISTS models;
    CREATE VIEW models AS
      SELECT s.id, datetime(s.start_us / 1e6, 'unixepoch', 'localtime') AS at, s.start_us, s.model, s.dur_ms,
        json_extract(s.attrs, '$."time_to_first_event_ns"') / 1e6 AS first_event_ms,
        json_extract(s.attrs, '$."time_to_first_output_ns"') / 1e6 AS first_output_ms,
        json_extract(s.attrs, '$."model.call_index"') AS call_index,
        json_extract(s.attrs, '$."model.attempts"') AS attempts,
        json_extract(s.attrs, '$."model.replayed"') AS replayed,
        json_extract(s.attrs, '$."model.response.status"') AS stop_reason,
        json_extract(s.attrs, '$."input_tokens"') AS input_tokens,
        json_extract(s.attrs, '$."cached_input_tokens"') AS cached_input_tokens,
        json_extract(s.attrs, '$."output_tokens"') AS output_tokens,
        json_extract(s.attrs, '$."reasoning.effort"') AS effort,
        s.status, s.error, s.session_id, s.thread_name, s.trace_id, s.span_id
      FROM spans s WHERE s.name = 'model.call';
    DROP VIEW IF EXISTS turns;
    CREATE VIEW turns AS
      SELECT s.id, datetime(s.start_us / 1e6, 'unixepoch', 'localtime') AS at, s.start_us, s.dur_ms, s.session_id,
        json_extract(s.attrs, '$."turn.index"') AS turn_index, s.model,
        json_extract(s.attrs, '$."agent.origin"') AS origin, json_extract(s.attrs, '$."agent.depth"') AS depth,
        json_extract(s.attrs, '$."parent.session.id"') AS parent_session_id,
        json_extract(s.attrs, '$."usage.total_tokens"') AS total_tokens, s.status, s.error, s.trace_id, s.span_id
      FROM spans s WHERE s.name = 'agent.turn';
    DROP VIEW IF EXISTS frames;
    CREATE VIEW frames AS
      SELECT s.id, s.start_us, s.dur_ms, json_extract(s.attrs, '$."thread.cpu_ns"') / 1e6 AS cpu_ms, s.session_id,
        json_extract(s.attrs, '$."frame.input_ms"') AS input_ms, json_extract(s.attrs, '$."frame.stream_ms"') AS stream_ms,
        json_extract(s.attrs, '$."frame.resize_ms"') AS resize_ms, json_extract(s.attrs, '$."frame.input"') AS input,
        json_extract(s.attrs, '$."frame.stream"') AS stream, json_extract(s.attrs, '$."frame.resize"') AS resize,
        s.pid, s.trace_id, s.span_id
      FROM spans s WHERE s.name = 'nc.frame';
  `);
  const sourceColumns = new Set(db.prepare("SELECT name FROM pragma_table_info('sources')").all().map((row) => row.name));
  if (!sourceColumns.has('executable')) db.exec('ALTER TABLE sources ADD COLUMN executable TEXT');
  if (!sourceColumns.has('built_us')) db.exec('ALTER TABLE sources ADD COLUMN built_us INTEGER');
  if (db.prepare('SELECT 1 FROM spans LIMIT 1').get()) ensureIndexes(db);
  return db;
}

function ensureIndexes(db) {
  db.exec(`
    CREATE INDEX IF NOT EXISTS spans_span ON spans(span_id);
    CREATE INDEX IF NOT EXISTS spans_parent ON spans(parent_id);
    CREATE INDEX IF NOT EXISTS spans_name ON spans(name, start_us);
    DROP INDEX IF EXISTS spans_session;
    CREATE INDEX IF NOT EXISTS spans_agent_session ON spans(session_id, name) WHERE session_id IS NOT NULL AND ui = 0;
    CREATE INDEX IF NOT EXISTS spans_unresolved ON spans(id) WHERE session_id IS NULL AND ui = 0;
    CREATE INDEX IF NOT EXISTS spans_trace ON spans(trace_id) WHERE ui = 0;
    CREATE INDEX IF NOT EXISTS spans_call ON spans(call_id) WHERE call_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS events_span ON events(span);
    CREATE INDEX IF NOT EXISTS metrics_name ON metrics(name, time_us);
  `);
}

function decode(value) {
  if (value === undefined || value === null) return null;
  if ('stringValue' in value) {
    const text = value.stringValue;
    return INTEGER.test(text) ? Number(text) : text;
  }
  if ('intValue' in value) return Number(value.intValue);
  if ('doubleValue' in value) return value.doubleValue;
  if ('boolValue' in value) return value.boolValue;
  if ('arrayValue' in value) return (value.arrayValue.values ?? []).map(decode);
  if ('kvlistValue' in value) return attributes(value.kvlistValue.values ?? []);
  if ('bytesValue' in value) return value.bytesValue;
  return null;
}

function attributes(list) {
  const result = {};
  for (const { key, value } of list ?? []) result[key] = decode(value);
  return result;
}

const micros = (nanos) => Math.round(Number(nanos) / 1000);

function sourceInfo(file) {
  const match = /(\d+)-(\d+)$/.exec(path.basename(path.dirname(file)));
  return match ? { pid: Number(match[2]), started_us: micros(match[1]) } : { pid: null, started_us: null };
}

function* lines(file, offset) {
  const fd = fs.openSync(file, 'r');
  try {
    const size = fs.fstatSync(fd).size;
    let position = offset;
    let carry = Buffer.alloc(0);
    while (position < size) {
      const chunk = Buffer.allocUnsafe(Math.min(CHUNK_BYTES, size - position));
      const read = fs.readSync(fd, chunk, 0, chunk.length, position);
      if (read === 0) break;
      const buffer = carry.length ? Buffer.concat([carry, chunk.subarray(0, read)]) : chunk.subarray(0, read);
      const base = position - carry.length;
      position += read;
      let start = 0;
      for (let end = buffer.indexOf(10, start); end !== -1; end = buffer.indexOf(10, start)) {
        if (end > start) yield { text: buffer.toString('utf8', start, end), next: base + end + 1 };
        start = end + 1;
      }
      carry = Buffer.from(buffer.subarray(start));
    }
  } finally {
    fs.closeSync(fd);
  }
}

function ingest(db, home, quiet) {
  const root = path.join(home, 'log', 'otel');
  if (!fs.existsSync(root)) fail('no telemetry directory at ' + root);
  const started = Date.now();
  const files = [];
  for (const dir of fs.readdirSync(root).sort()) {
    for (const kind of ['traces', 'metrics']) {
      const file = path.join(root, dir, kind + '.jsonl');
      if (fs.existsSync(file)) files.push({ file, kind });
    }
  }
  const getSource = db.prepare('SELECT id, offset FROM sources WHERE path = ?');
  const addSource = db.prepare('INSERT INTO sources(path, kind, pid, started_us) VALUES (?, ?, ?, ?) RETURNING id');
  const setOffset = db.prepare(
    'UPDATE sources SET offset = ?, service_version = coalesce(?, service_version) WHERE id = ?');
  const setBuild = db.prepare(
    'UPDATE sources SET executable = coalesce(executable, ?), built_us = coalesce(built_us, ?) WHERE id = ?');
  const addSpan = db.prepare(`INSERT INTO spans(source, pid, trace_id, span_id, parent_id, name, kind, start_us,
    end_us, dur_ms, span_ms, busy_ms, status, error, thread_id, thread_name, session_id, session_from, tool, call_id,
    model, callsite, ui, attrs) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  const addEvent = db.prepare('INSERT INTO events(span, time_us, name, kind, content, attrs) VALUES (?, ?, ?, ?, ?, ?)');
  const addLink = db.prepare('INSERT INTO links(span, trace_id, span_id) VALUES (?, ?, ?)');
  const addMetric = db.prepare(`INSERT INTO metrics(source, name, unit, type, time_us, start_us, attrs, value, count,
    sum, min, max, bounds, counts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  const callsites = new Map(db.prepare('SELECT id, target, file, line, module FROM callsites').all()
    .map((row) => [[row.target, row.file, row.line, row.module].join('|'), row.id]));
  const addCallsite = db.prepare('INSERT INTO callsites(target, file, line, module) VALUES (?, ?, ?, ?) RETURNING id');
  const callsite = (attrs) => {
    const fields = [attrs.target ?? null, attrs['code.file.path'] ?? null, attrs['code.line.number'] ?? null,
      attrs['code.module.name'] ?? null];
    const key = fields.join('|');
    let id = callsites.get(key);
    if (id === undefined) {
      id = addCallsite.get(...fields).id;
      callsites.set(key, id);
    }
    return id;
  };
  const mark = db.prepare('SELECT coalesce(max(id), 0) AS id FROM spans').get().id;
  let spanCount = 0;
  let metricCount = 0;
  let bytes = 0;

  const insertSpan = (span, sourceId, pid) => {
    const attrs = attributes(span.attributes);
    const startUs = micros(span.startTimeUnixNano);
    const endUs = micros(span.endTimeUnixNano);
    const spanMs = (endUs - startUs) / 1000;
    const explicitMs = typeof attrs.duration_ns === 'number' ? attrs.duration_ns / 1e6 : 0;
    const busyMs = typeof attrs.busy_ns === 'number' ? attrs.busy_ns / 1e6 : null;
    const code = span.status?.code ?? 0;
    const status = typeof attrs.status === 'string' ? attrs.status : code === 2 ? 'error' : code === 1 ? 'ok' : null;
    let tool = typeof attrs['tool.name'] === 'string' ? attrs['tool.name'] : null;
    if (span.name === 'code_mode.cell') tool = 'exec';
    if (span.name === 'code_mode.wait') tool = 'wait';
    if (span.name === 'mcp.tool_call') tool = attrs['mcp.server'] + '/' + attrs['mcp.tool'];
    const rest = {};
    for (const key in attrs) if (!COLUMN_ATTRIBUTES.has(key)) rest[key] = attrs[key];
    const sessionId = typeof attrs['session.id'] === 'string' && attrs['session.id'] ? attrs['session.id'] : null;
    const callId = attrs['tool.call_id'] === undefined || attrs['tool.call_id'] === null
      ? null : String(attrs['tool.call_id']);
    const row = addSpan.run(sourceId, pid, span.traceId, span.spanId, span.parentSpanId || null, span.name,
      span.kind ?? null, startUs, endUs, Math.max(spanMs, explicitMs), spanMs, busyMs, status, code === 2 ? 1 : 0,
      attrs['thread.id'] ?? null, attrs['thread.name'] ?? null, sessionId, sessionId ? 'span' : null, tool, callId,
      typeof attrs.model === 'string' ? attrs.model : null, callsite(attrs), isUi(span.name) ? 1 : 0,
      JSON.stringify(rest));
    const id = Number(row.lastInsertRowid);
    for (const event of span.events ?? []) {
      const eventAttrs = attributes(event.attributes);
      if (eventAttrs.target === 'nc_metrics') continue;
      const content = eventAttrs.content;
      const contentKind = eventAttrs.content_kind ?? null;
      for (const key of ['content', 'content_kind', 'code.file.path', 'code.module.name', 'code.line.number']) {
        delete eventAttrs[key];
      }
      addEvent.run(id, micros(event.timeUnixNano), event.name ?? null, contentKind,
        content === undefined || content === null ? null : String(content), JSON.stringify(eventAttrs));
    }
    for (const link of span.links ?? []) addLink.run(id, link.traceId, link.spanId);
  };

  const insertMetric = (metric, sourceId) => {
    let count = 0;
    for (const type of ['histogram', 'sum', 'gauge', 'exponentialHistogram']) {
      for (const point of metric[type]?.dataPoints ?? []) {
        const value = point.asDouble ?? (point.asInt !== undefined ? Number(point.asInt) : null);
        addMetric.run(sourceId, metric.name, metric.unit ?? null, type, micros(point.timeUnixNano),
          micros(point.startTimeUnixNano ?? 0), JSON.stringify(attributes(point.attributes)), value,
          point.count !== undefined ? Number(point.count) : null, point.sum ?? null, point.min ?? null,
          point.max ?? null, point.explicitBounds ? JSON.stringify(point.explicitBounds) : null,
          point.bucketCounts ? JSON.stringify(point.bucketCounts.map(Number)) : null);
        count++;
      }
    }
    return count;
  };

  for (const { file, kind } of files) {
    const size = fs.statSync(file).size;
    const info = sourceInfo(file);
    let source = getSource.get(file);
    if (!source) source = { id: addSource.get(file, kind, info.pid, info.started_us).id, offset: 0 };
    if (size <= source.offset) continue;
    let version = null;
    let offset = source.offset;
    let pending = 0;
    db.exec('BEGIN');
    for (const line of lines(file, source.offset)) {
      let request = null;
      try {
        request = JSON.parse(line.text);
      } catch (error) {
        console.error('otel: skipping unparsable line at ' + file + ' byte ' + offset + ': ' + error.message);
      }
      if (request && kind === 'traces') {
        for (const resourceSpans of request.resourceSpans ?? []) {
          if (version === null) {
            const resource = attributes(resourceSpans.resource?.attributes);
            version = resource['service.version'] ?? null;
            setBuild.run(resource['process.executable.path'] ?? null, resource['nanocode.executable.modified'] ?? null,
              source.id);
          }
          for (const scopeSpans of resourceSpans.scopeSpans ?? []) {
            for (const span of scopeSpans.spans ?? []) {
              insertSpan(span, source.id, info.pid);
              spanCount++;
            }
          }
        }
      } else if (request) {
        for (const resourceMetrics of request.resourceMetrics ?? []) {
          for (const scopeMetrics of resourceMetrics.scopeMetrics ?? []) {
            for (const metric of scopeMetrics.metrics ?? []) metricCount += insertMetric(metric, source.id);
          }
        }
      }
      bytes += line.next - offset;
      offset = line.next;
      if (++pending >= 2000) {
        setOffset.run(offset, version, source.id);
        db.exec('COMMIT; BEGIN');
        pending = 0;
      }
    }
    setOffset.run(offset, version, source.id);
    db.exec('COMMIT');
  }
  ensureIndexes(db);
  if (spanCount > 0) {
    inheritSessions(db, mark);
    rebuildSessions(db, mark);
  }
  if (!quiet && (spanCount > 0 || metricCount > 0)) {
    const seconds = ((Date.now() - started) / 1000).toFixed(1);
    console.error(`otel: ingested ${spanCount} spans and ${metricCount} metric points from ${(bytes / 1e6).toFixed(1)} MB in ${seconds}s`);
  }
  return { spans: spanCount, metrics: metricCount };
}

function inheritSessions(db, mark) {
  const fromParent = db.prepare(`UPDATE spans INDEXED BY spans_unresolved
    SET session_id = parent.session_id, session_from = 'parent'
    FROM spans AS parent
    WHERE spans.session_id IS NULL AND spans.ui = 0 AND parent.span_id = spans.parent_id
      AND parent.trace_id = spans.trace_id AND parent.session_id IS NOT NULL`);
  const fromTrace = db.prepare(`UPDATE spans SET session_id = trace.session_id, session_from = 'trace'
    FROM (
      SELECT trace_id, min(session_id) AS session_id FROM spans
      WHERE ui = 0 AND session_id IS NOT NULL
        AND trace_id IN (SELECT trace_id FROM spans WHERE id > ? AND ui = 0)
      GROUP BY trace_id HAVING count(DISTINCT session_id) = 1
    ) AS trace
    WHERE spans.session_id IS NULL AND spans.ui = 0 AND spans.trace_id = trace.trace_id`);
  db.exec('BEGIN');
  while (fromParent.run().changes > 0);
  fromTrace.run(mark);
  db.exec('COMMIT');
}

function rebuildSessions(db, mark) {
  db.exec('BEGIN');
  db.exec('CREATE TEMP TABLE IF NOT EXISTS touched(session_id TEXT PRIMARY KEY); DELETE FROM touched;');
  db.prepare(`INSERT OR IGNORE INTO touched SELECT DISTINCT session_id FROM spans
    WHERE id > ? AND session_id IS NOT NULL AND ui = 0`).run(mark);
  db.exec(`
    DELETE FROM sessions WHERE session_id IN (SELECT session_id FROM touched);
    INSERT INTO sessions(session_id, first_us, last_us, spans, turns, model_calls, model_ms, cells, cell_ms,
        tool_calls, tool_ms)
      SELECT session_id, min(start_us), max(end_us), count(*), sum(name = 'agent.turn'), sum(name = 'model.call'),
        sum(CASE WHEN name = 'model.call' THEN dur_ms END),
        sum(name IN ('code_mode.cell', 'code_mode.wait')),
        sum(CASE WHEN name IN ('code_mode.cell', 'code_mode.wait') THEN dur_ms END),
        sum(name = 'tool.execute'), sum(CASE WHEN name = 'tool.execute' THEN dur_ms END)
      FROM spans WHERE ui = 0 AND session_id IN (SELECT session_id FROM touched)
      GROUP BY session_id;
    UPDATE sessions SET root_session_id = coalesce(t.root, t.lineage), parent_session_id = t.parent,
      origin = t.origin, depth = t.depth, model = t.model, effort = t.effort
    FROM (
      SELECT session_id, json_extract(attrs, '$."session.root_id"') AS root,
        json_extract(attrs, '$."session.lineage_id"') AS lineage,
        json_extract(attrs, '$."parent.session.id"') AS parent, json_extract(attrs, '$."agent.origin"') AS origin,
        json_extract(attrs, '$."agent.depth"') AS depth, model, json_extract(attrs, '$."reasoning.effort"') AS effort,
        max(start_us)
      FROM spans WHERE ui = 0 AND name = 'agent.turn' AND session_id IN (SELECT session_id FROM touched)
      GROUP BY session_id
    ) AS t WHERE t.session_id = sessions.session_id;
    UPDATE sessions SET model = m.model, effort = coalesce(sessions.effort, m.effort) FROM (
      SELECT session_id, model, json_extract(attrs, '$."reasoning.effort"') AS effort, max(start_us)
      FROM spans WHERE ui = 0 AND name = 'model.call' AND session_id IN (SELECT session_id FROM touched)
      GROUP BY session_id
    ) AS m WHERE sessions.model IS NULL AND m.session_id = sessions.session_id;
  `);
  db.exec('COMMIT');
}

function sinceMicros(value) {
  if (value === undefined) return null;
  const relative = /^(\d+(?:\.\d+)?)(s|m|h|d|w)$/.exec(value);
  if (relative) {
    const unit = { s: 1, m: 60, h: 3600, d: 86400, w: 604800 }[relative[2]];
    return Math.round((Date.now() / 1000 - Number(relative[1]) * unit) * 1e6);
  }
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) fail('invalid --since ' + value);
  return parsed * 1000;
}

function keySql(key) {
  const alias = { session: 'session_id', thread: 'thread_name', process: 'pid' }[key] ?? key;
  if (COLUMNS.has(alias)) return 's.' + alias;
  if (alias === 'build') {
    return `(SELECT coalesce(replace(src.executable, rtrim(src.executable, replace(src.executable, '/', '')), ''), '-')
      || ' @ ' || coalesce(strftime('%m-%d %H:%M:%S', src.built_us / 1e6, 'unixepoch', 'localtime'), '-')
      FROM sources src WHERE src.id = s.source)`;
  }
  return `json_extract(s.attrs, '$."${alias.replaceAll('"', '""').replaceAll("'", "''")}"')`;
}

function groupKeys(options, span) {
  if (options.by === 'none') return [];
  if (options.by) return options.by.split(',').map((key) => key.trim()).filter(Boolean);
  if (span.includes('%')) return ['name'];
  return DEFAULT_GROUP[span] ?? (span.startsWith('code_mode.') ? ['name'] : []);
}

function spanFilter(options, defaultSpan = 'tool.execute') {
  const clauses = [];
  const params = [];
  const span = options.span ?? defaultSpan;
  if (span) {
    clauses.push(span.includes('%') ? 's.name LIKE ?' : 's.name = ?');
    params.push(span);
  }
  const since = sinceMicros(options.since);
  if (since !== null) {
    clauses.push('s.start_us >= ?');
    params.push(since);
  }
  if (options.session) {
    clauses.push("s.session_id LIKE ? || '%'");
    params.push(options.session);
  }
  if (options.where) clauses.push('(' + options.where + ')');
  return { span, where: clauses.length ? clauses.join(' AND ') : '1', params };
}

function groupSql(keys) {
  if (keys.length === 0) return "'all'";
  return keys.map((key) => `coalesce(CAST(${keySql(key)} AS TEXT), '-')`).join(" || ' · ' || ");
}

function quantile(sorted, q) {
  if (sorted.length === 0) return null;
  const position = (sorted.length - 1) * q;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  return sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower);
}

function groupStats(rows) {
  const groups = new Map();
  for (const row of rows) {
    let group = groups.get(row.grp);
    if (!group) groups.set(row.grp, (group = { name: row.grp, values: [], errors: 0 }));
    group.values.push(row.dur_ms);
    group.errors += row.error;
  }
  for (const group of groups.values()) {
    group.values.sort((a, b) => a - b);
    const values = group.values;
    group.n = values.length;
    group.total = values.reduce((sum, value) => sum + value, 0);
    group.p50 = quantile(values, 0.5);
    group.p90 = quantile(values, 0.9);
    group.p99 = quantile(values, 0.99);
    group.p999 = quantile(values, 0.999);
    group.max = values[values.length - 1];
  }
  return [...groups.values()].sort((a, b) => b.total - a.total);
}

function fmtMs(ms) {
  if (ms === null || ms === undefined || Number.isNaN(ms)) return '-';
  const abs = Math.abs(ms);
  if (abs < 1) return (ms * 1000).toFixed(abs < 0.01 ? 1 : 0) + 'µs';
  if (abs < 1000) return ms.toFixed(abs < 10 ? 2 : abs < 100 ? 1 : 0) + 'ms';
  if (abs < 60_000) return (ms / 1000).toFixed(abs < 10_000 ? 2 : 1) + 's';
  if (abs < 3_600_000) return (ms / 60_000).toFixed(1) + 'm';
  return (ms / 3_600_000).toFixed(1) + 'h';
}

function fmtTime(us) {
  const date = new Date(us / 1000);
  const pad = (value, width = 2) => String(value).padStart(width, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

const shortSession = (id) => (id ? id.slice(0, 13) : '-');

function shorten(value, width, options) {
  const text = value === null || value === undefined ? '' : String(value).replace(/\s+/g, ' ');
  if (options.full || text.length <= width) return text;
  return text.slice(0, width - 1) + '…';
}

function printTable(rows, options, widths = {}) {
  if (options.json) {
    console.log(JSON.stringify(rows, (_, value) => (typeof value === 'bigint' ? value.toString() : value), 2));
    return;
  }
  if (rows.length === 0) {
    console.log('(no rows)');
    return;
  }
  const columns = Object.keys(rows[0]);
  const cells = rows.map((row) => columns.map((column) => {
    const value = row[column];
    if (typeof value === 'bigint') return value.toString();
    if (typeof value === 'number' && !Number.isInteger(value)) return String(Number(value.toPrecision(6)));
    return shorten(value, widths[column] ?? 120, options);
  }));
  const numeric = columns.map((_, index) => rows.every((row) => {
    const value = row[columns[index]];
    return value === null || typeof value === 'number' || typeof value === 'bigint';
  }));
  const sizes = columns.map((column, index) =>
    Math.max(column.length, ...cells.map((row) => row[index].length)));
  const line = (values) => values.map((value, index) =>
    numeric[index] ? value.padStart(sizes[index]) : value.padEnd(sizes[index])).join('  ').trimEnd();
  console.log(line(columns));
  console.log(sizes.map((size) => '─'.repeat(size)).join('  '));
  for (const row of cells) console.log(line(row));
}

const BUCKETS_PER_DECADE = 4;
const bucketOf = (ms) => Math.floor(Math.log10(Math.max(ms, 1e-3)) * BUCKETS_PER_DECADE);
const bucketStart = (bucket) => 10 ** (bucket / BUCKETS_PER_DECADE);
const BLOCKS = ' ▁▂▃▄▅▆▇█';

function histogram(values) {
  const counts = new Map();
  for (const value of values) {
    const bucket = bucketOf(value);
    counts.set(bucket, (counts.get(bucket) ?? 0) + 1);
  }
  return counts;
}

function sparkline(counts, low, high) {
  const peak = Math.max(...counts.values());
  let line = '';
  for (let bucket = low; bucket <= high; bucket++) {
    const count = counts.get(bucket) ?? 0;
    line += count === 0 ? '·' : BLOCKS[Math.max(1, Math.round((count / peak) * 8))];
  }
  return line;
}

function decadeLabel(ms) {
  if (ms < 1) return Math.round(ms * 1000) + 'µs';
  if (ms < 1000) return Math.round(ms) + 'ms';
  return Math.round(ms / 1000) + 's';
}

function sparkAxis(low, high, indent) {
  const axis = ' '.repeat(high - low + 1).split('');
  const labels = [];
  for (let bucket = low; bucket <= high; bucket++) {
    if (bucket % BUCKETS_PER_DECADE === 0) {
      axis[bucket - low] = '╵';
      labels.push([bucket - low, decadeLabel(bucketStart(bucket))]);
    }
  }
  const labelLine = ' '.repeat(indent + high - low + 8).split('');
  let lastEnd = -1;
  for (const [position, label] of labels) {
    const start = indent + position;
    if (start <= lastEnd) continue;
    for (let index = 0; index < label.length; index++) labelLine[start + index] = label[index];
    lastEnd = start + label.length;
  }
  return ' '.repeat(indent) + axis.join('').trimEnd() + '\n' + labelLine.join('').trimEnd();
}

function loadRows(db, options, defaultSpan) {
  const filter = spanFilter(options, defaultSpan);
  const keys = groupKeys(options, filter.span ?? '');
  const rows = db.prepare(`SELECT ${groupSql(keys)} AS grp, s.dur_ms, s.error FROM spans s WHERE ${filter.where}`)
    .all(...filter.params);
  return { filter, keys, rows };
}

function tails(db, options) {
  const { filter, keys, rows } = loadRows(db, options);
  const groups = groupStats(rows).slice(0, Number(options.limit ?? 25));
  if (options.json) return printTable(groups.map(({ values, ...group }) => group), options);
  const label = (filter.span ?? 'all spans') + (keys.length ? ' by ' + keys.join(', ') : '');
  console.log(`${label}: ${rows.length} spans${options.since ? ' since ' + options.since : ''}\n`);
  if (groups.length === 0) return console.log('(no spans)');
  const histograms = groups.map((group) => histogram(group.values));
  const low = Math.min(...histograms.flatMap((counts) => [...counts.keys()]));
  const high = Math.max(...histograms.flatMap((counts) => [...counts.keys()]));
  const nameWidth = Math.min(36, Math.max(5, ...groups.map((group) => group.name.length)));
  const header = ['group'.padEnd(nameWidth), 'n'.padStart(7), 'p50'.padStart(8), 'p90'.padStart(8),
    'p99'.padStart(8), 'p99.9'.padStart(8), 'max'.padStart(8), 'total'.padStart(8), 'err'.padStart(5), '  distribution (log scale)'];
  console.log(header.join(' '));
  groups.forEach((group, index) => {
    console.log([
      shorten(group.name, nameWidth, options).padEnd(nameWidth), String(group.n).padStart(7),
      fmtMs(group.p50).padStart(8), fmtMs(group.p90).padStart(8), fmtMs(group.p99).padStart(8),
      fmtMs(group.p999).padStart(8), fmtMs(group.max).padStart(8), fmtMs(group.total).padStart(8),
      String(group.errors).padStart(5), '  ' + sparkline(histograms[index], low, high),
    ].join(' '));
  });
  console.log(sparkAxis(low, high, nameWidth + 1 + 7 + 1 + 6 * 9 + 5 + 1 + 2));
  if (!options.hist) return;
  groups.forEach((group, index) => {
    console.log('\n' + group.name + ` (n=${group.n}, p50 ${fmtMs(group.p50)}, p99 ${fmtMs(group.p99)})`);
    const counts = histograms[index];
    const peak = Math.max(...counts.values());
    let cumulative = 0;
    const first = Math.min(...counts.keys());
    const last = Math.max(...counts.keys());
    for (let bucket = first; bucket <= last; bucket++) {
      const count = counts.get(bucket) ?? 0;
      cumulative += count;
      const range = (fmtMs(bucketStart(bucket)) + '–' + fmtMs(bucketStart(bucket + 1))).padStart(17);
      const bar = '█'.repeat(Math.round((count / peak) * 40)) || (count ? '▏' : '');
      console.log(`  ${range} │${bar.padEnd(40)} ${String(count).padStart(6)}  ${((cumulative / group.n) * 100).toFixed(1).padStart(5)}%`);
    }
  });
}

function detailFor(db, row) {
  const args = db.prepare("SELECT content FROM events WHERE span = ? AND kind = 'tool.arguments' LIMIT 1").get(row.id);
  if (args?.content) {
    try {
      const parsed = JSON.parse(args.content);
      if (parsed && typeof parsed === 'object') {
        const main = parsed.command ?? parsed.cmd ?? parsed.file_path ?? parsed.path ?? parsed.pattern ?? parsed.code;
        if (main !== undefined) return String(main);
      }
    } catch {}
    return args.content;
  }
  const attrs = JSON.parse(row.attrs);
  for (const key of ['duration_ns', 'tool.arguments.keys', 'tool.arguments.kind', 'tool.arguments.count']) {
    delete attrs[key];
  }
  return JSON.stringify(attrs);
}

function findOutliers(db, options) {
  const filter = spanFilter(options);
  const keys = groupKeys(options, filter.span ?? '');
  const above = Number(options.above ?? 99) / 100;
  const minimum = Number(options.min ?? 20);
  const rows = db.prepare(`SELECT s.id, ${groupSql(keys)} AS grp, s.name, s.dur_ms, s.error, s.start_us, s.end_us,
      s.pid, s.session_id, s.thread_name, s.thread_id, s.call_id, s.status, s.attrs, c.file, c.line
    FROM spans s LEFT JOIN callsites c ON c.id = s.callsite WHERE ${filter.where}`).all(...filter.params);
  const groups = new Map(groupStats(rows).map((group) => [group.name, group]));
  const skipped = [...groups.values()].filter((group) => group.n < minimum);
  const found = [];
  for (const row of rows) {
    const group = groups.get(row.grp);
    if (group.n < minimum) continue;
    const threshold = quantile(group.values, above);
    if (row.dur_ms > threshold) found.push({ row, group, threshold });
  }
  found.sort((a, b) => (b.row.dur_ms - b.group.p50) - (a.row.dur_ms - a.group.p50));
  return { filter, keys, found, skipped, minimum };
}

function outlierRow(db, { row, group, threshold }, options) {
  const elsewhere = db.prepare(`SELECT count(DISTINCT pid) AS n FROM spans
    WHERE name = ? AND pid != ? AND start_us > ? AND start_us < ? AND end_us > ? AND dur_ms > ?`)
    .get(row.name, row.pid, row.start_us - Math.ceil(group.max * 1000), row.end_us, row.start_us, threshold).n;
  return {
    id: row.id,
    at: fmtTime(row.start_us),
    group: row.grp,
    dur: fmtMs(row.dur_ms),
    'x p50': Number((row.dur_ms / Math.max(group.p50, 1e-6)).toFixed(1)),
    excess: fmtMs(row.dur_ms - group.p50),
    threshold: fmtMs(threshold),
    status: row.status ?? '-',
    session: options.json ? row.session_id : shortSession(row.session_id),
    thread: (row.thread_name ?? '-') + ':' + (row.thread_id ?? '-'),
    pid: row.pid,
    'slow elsewhere': elsewhere,
    call: row.call_id ?? '-',
    detail: detailFor(db, row),
    code: row.file ? row.file + ':' + row.line : '-',
  };
}

function outliers(db, options) {
  const { filter, keys, found, skipped, minimum } = findOutliers(db, options);
  const limited = found.slice(0, Number(options.limit ?? 25));
  if (!options.json) {
    const label = (filter.span ?? 'all spans') + (keys.length ? ' by ' + keys.join(', ') : '');
    console.log(`${label}: ${found.length} spans above p${options.above ?? 99} of their group (showing ${limited.length}, sorted by time above the group median)`);
    if (skipped.length) {
      console.log(`groups with fewer than ${minimum} spans are not thresholded: ${skipped.map((group) => `${group.name} (n=${group.n}, max ${fmtMs(group.max)})`).join(', ')}`);
    }
    console.log('');
  }
  printTable(limited.map((entry) => outlierRow(db, entry, options)), options, { detail: 90, call: 40, group: 30 });
}

function resolveSpan(db, id) {
  if (/^\d+$/.test(id)) return db.prepare('SELECT * FROM spans WHERE id = ?').get(Number(id));
  if (/^[0-9a-f]{16}$/.test(id)) return db.prepare('SELECT * FROM spans WHERE span_id = ? ORDER BY id DESC').get(id);
  return db.prepare(`SELECT * FROM spans WHERE call_id = ?
    ORDER BY name = 'tool.execute' DESC, name = 'code_mode.cell' DESC, name = 'tool.call' DESC, id DESC`).get(id);
}

function spanSummary(span) {
  const parts = [span.name];
  if (span.tool && span.name !== 'code_mode.cell' && span.name !== 'code_mode.wait') parts.push(span.tool);
  if (span.model) parts.push(span.model);
  if (span.status) parts.push(span.status);
  return parts.join(' ');
}

function trace(db, options) {
  const id = options._[1];
  if (!id) fail('trace needs a span row id, span id or tool call id');
  const span = resolveSpan(db, id);
  if (!span) fail('no span matches ' + id);
  const parent = db.prepare('SELECT * FROM spans WHERE trace_id = ? AND span_id = ? ORDER BY id DESC');
  const ancestors = [];
  for (let current = span; current.parent_id;) {
    const next = parent.get(current.trace_id, current.parent_id);
    if (!next) {
      ancestors.push({ missing: current.parent_id });
      break;
    }
    ancestors.push(next);
    current = next;
  }
  console.log(`span ${span.id} ${spanSummary(span)} ${fmtMs(span.dur_ms)} at ${fmtTime(span.start_us)}`);
  console.log(`  session ${span.session_id ?? '-'} (${span.session_from ?? 'none'}), thread ${span.thread_name}:${span.thread_id}, pid ${span.pid}, trace ${span.trace_id}`);
  const attrs = JSON.parse(span.attrs);
  console.log('  attributes ' + JSON.stringify(attrs));
  for (const event of db.prepare('SELECT time_us, name, kind, content FROM events WHERE span = ? ORDER BY time_us').all(span.id)) {
    console.log(`  event +${fmtMs((event.time_us - span.start_us) / 1000)} ${event.kind ?? event.name}: ${shorten(event.content ?? '', 300, options)}`);
  }
  if (ancestors.length) {
    console.log('\nancestors:');
    for (const ancestor of ancestors) {
      if (ancestor.missing) {
        console.log(`  ${ancestor.missing} (not exported: the parent span had not ended when telemetry was written)`);
      } else {
        console.log(`  ${String(ancestor.id).padStart(9)} ${fmtMs(ancestor.dur_ms).padStart(8)}  ${spanSummary(ancestor)}  session ${shortSession(ancestor.session_id)}`);
      }
    }
  }
  const tree = db.prepare(`WITH RECURSIVE tree(id, depth, path) AS (
      SELECT id, 0, printf('%020d', start_us) FROM spans WHERE id = ?
      UNION ALL
      SELECT child.id, tree.depth + 1, tree.path || '/' || printf('%020d', child.start_us)
      FROM tree JOIN spans AS node ON node.id = tree.id
      JOIN spans AS child ON child.parent_id = node.span_id AND child.trace_id = node.trace_id
    )
    SELECT s.id, tree.depth, s.name, s.tool, s.model, s.status, s.start_us, s.dur_ms, s.span_ms, s.call_id,
      s.thread_name, s.thread_id, s.parent_id, s.span_id
    FROM tree JOIN spans s ON s.id = tree.id ORDER BY tree.path`).all(span.id);
  const childTotals = new Map();
  for (const node of tree) {
    if (node.depth === 0) continue;
    const parentNode = tree.find((candidate) => candidate.span_id === node.parent_id);
    if (parentNode) childTotals.set(parentNode.id, (childTotals.get(parentNode.id) ?? 0) + node.dur_ms);
  }
  const limit = Number(options.limit ?? 200);
  console.log(`\ndescendants (${tree.length - 1}; offset from this span's start, duration, self time = duration minus children):`);
  for (const node of tree.slice(0, limit + 1)) {
    const self = Math.max(0, node.dur_ms - (childTotals.get(node.id) ?? 0));
    const detail = node.call_id ? ' ' + node.call_id : '';
    console.log(`  ${('+' + fmtMs((node.start_us - span.start_us) / 1000)).padStart(9)} ${fmtMs(node.dur_ms).padStart(8)} self ${fmtMs(self).padStart(8)}  ${'  '.repeat(node.depth)}${spanSummary(node)}${detail} [${node.id}]`);
  }
  if (tree.length > limit + 1) console.log(`  … ${tree.length - limit - 1} more (raise --limit)`);
}

function prune(db, options) {
  if (options['older-than'] === undefined) fail('prune needs --older-than, for example --older-than 2d');
  const cutoff = sinceMicros(options['older-than']) / 1000;
  const root = path.join(homeDir(options), 'log', 'otel');
  const offsets = new Map(db.prepare('SELECT path, offset FROM sources').all().map((row) => [row.path, row.offset]));
  let freed = 0;
  let removed = 0;
  const kept = [];
  const names = fs.readdirSync(root).sort();
  const newest = new Map();
  for (const name of names) {
    const match = /^(\d+)-(\d+)$/.exec(name);
    if (match && (newest.get(match[2]) ?? '') < name) newest.set(match[2], name);
  }
  for (const name of names) {
    const directory = path.join(root, name);
    const pid = Number(/-(\d+)$/.exec(name)?.[1]);
    const files = fs.readdirSync(directory).map((file) => path.join(directory, file));
    const stats = files.map((file) => fs.statSync(file));
    const modified = Math.max(0, ...stats.map((stat) => stat.mtimeMs));
    const bytes = stats.reduce((sum, stat) => sum + stat.size, 0);
    let running = false;
    if (pid > 0 && newest.get(String(pid)) === name) {
      try {
        process.kill(pid, 0);
        running = true;
      } catch (error) {
        running = error.code === 'EPERM';
      }
    }
    const ingested = files.every((file, index) => !file.endsWith('.jsonl') || offsets.get(file) === stats[index].size);
    if (running || modified > cutoff || !ingested) {
      if (modified <= cutoff) kept.push(`${name} (${running ? 'process running' : 'not fully ingested'})`);
      continue;
    }
    if (!options['dry-run']) fs.rmSync(directory, { recursive: true, force: true });
    freed += bytes;
    removed++;
  }
  console.log(`${options['dry-run'] ? 'would remove' : 'removed'} ${removed} telemetry directories, ${(freed / 1e9).toFixed(2)} GB; their spans stay in the cache`);
  if (kept.length) console.log('kept: ' + kept.join(', '));
}

function breakdown(db, options) {
  const since = sinceMicros(options.since);
  const turns = db.prepare(`SELECT id, session_id, model, start_us, end_us FROM spans
    WHERE ui = 0 AND name = 'agent.turn' AND session_id IS NOT NULL AND (? IS NULL OR start_us >= ?)
      AND (? IS NULL OR session_id LIKE ? || '%') ORDER BY start_us`).all(since, since, options.session ?? null, options.session ?? null);
  const parts = db.prepare(`SELECT name, start_us, end_us FROM spans INDEXED BY spans_agent_session
    WHERE session_id = ? AND ui = 0 AND name IN ('model.call', 'model.compaction', 'model.warmup', 'code_mode.cell',
      'code_mode.cell_actor', 'code_mode.wait', 'tool.call', 'nc.tool_call', 'durability.store')
      AND start_us < ? AND end_us > ?`);
  const rank = { 'model.call': 0, 'model.compaction': 0, 'model.warmup': 0, 'code_mode.cell': 1,
    'code_mode.cell_actor': 1, 'code_mode.wait': 1, 'tool.call': 1, 'nc.tool_call': 1, 'durability.store': 2 };
  const labels = ['model calls', 'tools', 'durability', 'other'];
  const key = options.by ?? 'session';
  const totals = new Map();
  for (const turn of turns) {
    const edges = [];
    for (const part of parts.all(turn.session_id, turn.end_us, turn.start_us)) {
      edges.push([Math.max(part.start_us, turn.start_us), rank[part.name], 1]);
      edges.push([Math.min(part.end_us, turn.end_us), rank[part.name], -1]);
    }
    edges.sort((a, b) => a[0] - b[0]);
    const open = [0, 0, 0];
    const spent = [0, 0, 0, 0];
    let cursor = turn.start_us;
    for (const [at, category, delta] of edges) {
      const active = open.findIndex((count) => count > 0);
      spent[active === -1 ? 3 : active] += at - cursor;
      open[category] += delta;
      cursor = at;
    }
    spent[3] += turn.end_us - cursor;
    const group = key === 'model' ? turn.model : key === 'none' ? 'all' : turn.session_id;
    let entry = totals.get(group);
    if (!entry) totals.set(group, (entry = { turns: 0, wall: 0, spent: [0, 0, 0, 0] }));
    entry.turns++;
    entry.wall += turn.end_us - turn.start_us;
    spent.forEach((value, index) => { entry.spent[index] += value; });
  }
  const rows = [...totals.entries()].sort((a, b) => b[1].wall - a[1].wall).slice(0, Number(options.limit ?? 25));
  printTable(rows.map(([group, entry]) => {
    const row = { [key === 'model' ? 'model' : key === 'none' ? 'scope' : 'session']: group, turns: entry.turns,
      wall: fmtMs(entry.wall / 1000) };
    labels.forEach((label, index) => {
      row[label] = fmtMs(entry.spent[index] / 1000) + ' ' + (100 * entry.spent[index] / Math.max(entry.wall, 1)).toFixed(1) + '%';
    });
    row['other per turn'] = fmtMs(entry.spent[3] / 1000 / entry.turns);
    return row;
  }), options);
}

function builds(db, options) {
  const since = sinceMicros(options.since);
  const rows = db.prepare(`SELECT coalesce(src.executable, '-') AS executable, src.built_us,
      count(DISTINCT src.pid) AS processes, min(src.started_us) AS first_us, max(src.started_us) AS last_us,
      group_concat(src.id) AS ids
    FROM sources src WHERE src.kind = 'traces' AND (? IS NULL OR src.started_us >= ?)
    GROUP BY src.executable, src.built_us ORDER BY last_us DESC LIMIT ?`).all(since, since, Number(options.limit ?? 25));
  const counts = db.prepare(`SELECT count(*) AS spans, count(DISTINCT session_id) AS sessions FROM spans
    WHERE source IN (SELECT value FROM json_each(?))`);
  printTable(rows.map((row) => {
    const { spans, sessions: sessionCount } = counts.get(JSON.stringify(row.ids.split(',').map(Number)));
    return {
      executable: row.executable,
      built: row.built_us ? fmtTime(row.built_us) : '-',
      processes: row.processes,
      first: fmtTime(row.first_us),
      last: fmtTime(row.last_us),
      sessions: sessionCount,
      spans,
    };
  }), options);
}

function sessions(db, options) {
  const since = sinceMicros(options.since);
  const rows = db.prepare(`SELECT session_id, model, effort, origin, depth, parent_session_id, first_us, last_us, turns,
      model_calls, model_ms, cells, cell_ms, tool_calls, tool_ms
    FROM sessions WHERE (? IS NULL OR last_us >= ?) AND (? IS NULL OR session_id LIKE ? || '%')
    ORDER BY last_us DESC LIMIT ?`).all(since, since, options.session ?? null, options.session ?? null,
    Number(options.limit ?? 25));
  const slowest = db.prepare(`SELECT tool, dur_ms, id FROM spans WHERE ui = 0 AND session_id = ? AND name = 'tool.execute'
    ORDER BY dur_ms DESC LIMIT 1`);
  printTable(rows.map((row) => {
    const top = slowest.get(row.session_id);
    return {
      session: row.session_id,
      model: row.model ?? '-',
      effort: row.effort ?? '-',
      origin: row.origin ?? '-',
      depth: row.depth ?? null,
      parent: shortSession(row.parent_session_id),
      first: fmtTime(row.first_us),
      last: fmtTime(row.last_us),
      turns: row.turns,
      'model calls': row.model_calls,
      'model time': fmtMs(row.model_ms ?? 0),
      cells: row.cells,
      'cell time': fmtMs(row.cell_ms ?? 0),
      tools: row.tool_calls,
      'tool time': fmtMs(row.tool_ms ?? 0),
      'slowest tool': top ? `${top.tool} ${fmtMs(top.dur_ms)} [${top.id}]` : '-',
    };
  }), options);
}

function schema(db, options) {
  const objects = db.prepare("SELECT type, name, sql FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY type, name").all();
  for (const object of objects) {
    const columns = db.prepare(`SELECT name FROM pragma_table_info('${object.name}')`).all().map((column) => column.name);
    console.log(`${object.type} ${object.name}(${columns.join(', ')})`);
  }
  const filter = spanFilter(options, null);
  const names = db.prepare(`SELECT s.name, count(*) AS n, min(s.start_us) AS first_us, max(s.start_us) AS last_us
    FROM spans s WHERE ${filter.where} GROUP BY s.name ORDER BY n DESC`).all(...filter.params);
  const attributed = new Map(db.prepare(`SELECT name, count(*) AS n FROM spans INDEXED BY spans_agent_session
    WHERE session_id IS NOT NULL AND ui = 0 GROUP BY name`).all().map((row) => [row.name, row.n]));
  const sample = db.prepare('SELECT attrs FROM spans WHERE name = ? ORDER BY id DESC LIMIT 200');
  console.log('\nspans (name, count, agent spans with a session or ui, attribute keys from recent spans):');
  for (const row of names) {
    const keys = new Set();
    for (const { attrs } of sample.all(row.name)) for (const key of Object.keys(JSON.parse(attrs))) keys.add(key);
    const session = isUi(row.name) ? 'ui' : attributed.get(row.name) ?? 0;
    console.log(`  ${row.name} n=${row.n} session=${session} ${fmtTime(row.first_us)} .. ${fmtTime(row.last_us)}`);
    console.log('    ' + [...keys].join(', '));
  }
  const kinds = db.prepare('SELECT kind, count(*) AS n FROM events GROUP BY kind ORDER BY n DESC').all();
  console.log('\nevent kinds: ' + kinds.map((row) => `${row.kind ?? '(log)'}=${row.n}`).join(', '));
  const metrics = db.prepare('SELECT name, type, count(*) AS n FROM metrics GROUP BY name, type ORDER BY name').all();
  console.log('metrics: ' + metrics.map((row) => `${row.name} (${row.type})`).join(', '));
}

function sql(db, options) {
  const query = options._.slice(1).join(' ');
  if (!query) fail('sql needs a query');
  const statement = db.prepare(query);
  statement.setReadBigInts(true);
  const rows = statement.all().map((row) => Object.fromEntries(Object.entries(row).map(([key, value]) => [
    key, typeof value === 'bigint' && value <= BigInt(Number.MAX_SAFE_INTEGER) && value >= -BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(value) : value,
  ])));
  printTable(rows, options);
}

const PALETTE = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#17becf', '#bcbd22'];
const OTHER = '#9a9a9a';

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"]/g, (character) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[character]);
}

function logScale(min, max, from, to) {
  const low = Math.log10(min);
  const span = Math.max(Math.log10(max) - low, 1e-9);
  return (value) => from + ((Math.log10(Math.max(value, min)) - low) / span) * (to - from);
}

function decades(min, max) {
  const ticks = [];
  for (let exponent = Math.floor(Math.log10(min)); exponent <= Math.ceil(Math.log10(max)); exponent++) {
    const tick = 10 ** exponent;
    if (tick >= min * 0.999 && tick <= max * 1.001) ticks.push(tick);
  }
  return ticks;
}

function ccdfSvg(groups) {
  const width = 720;
  const height = 340;
  const margin = { left: 64, right: 16, top: 12, bottom: 40 };
  const positive = groups.flatMap((group) => [group.values[0], group.max]).filter((value) => value > 0);
  const xMin = Math.max(Math.min(...positive), 1e-3);
  const xMax = Math.max(Math.max(...positive), xMin * 10);
  const yMin = 1 / Math.max(...groups.map((group) => group.n));
  const x = logScale(xMin, xMax, margin.left, width - margin.right);
  const y = logScale(yMin, 1, height - margin.bottom, margin.top);
  let svg = `<svg viewBox="0 0 ${width} ${height}" width="${width}" height="${height}">`;
  for (const tick of decades(xMin, xMax)) {
    svg += `<line x1="${x(tick)}" x2="${x(tick)}" y1="${margin.top}" y2="${height - margin.bottom}" class="grid"/>`;
    svg += `<text x="${x(tick)}" y="${height - margin.bottom + 16}" text-anchor="middle">${decadeLabel(tick)}</text>`;
  }
  for (const [probability, label] of [[0.5, 'p50'], [0.1, 'p90'], [0.01, 'p99'], [0.001, 'p99.9'], [0.0001, 'p99.99']]) {
    if (probability < yMin) continue;
    svg += `<line x1="${margin.left}" x2="${width - margin.right}" y1="${y(probability)}" y2="${y(probability)}" class="grid"/>`;
    svg += `<text x="${margin.left - 6}" y="${y(probability) + 4}" text-anchor="end">${label}</text>`;
  }
  svg += `<text x="${(margin.left + width - margin.right) / 2}" y="${height - 6}" text-anchor="middle">duration (log)</text>`;
  svg += `<text x="12" y="${margin.top + 8}">P(≥x)</text>`;
  groups.forEach((group, index) => {
    const values = group.values;
    const n = values.length;
    const step = Math.max(1, Math.ceil(n / 600));
    const points = [];
    for (let i = 0; i < n; i++) {
      if (i % step !== 0 && n - i > 300) continue;
      points.push(`${x(values[i]).toFixed(1)},${y((n - i) / n).toFixed(1)}`);
    }
    svg += `<polyline fill="none" stroke="${PALETTE[index % PALETTE.length]}" stroke-width="1.6" points="${points.join(' ')}"><title>${escapeHtml(group.name)} n=${n} p50=${fmtMs(group.p50)} p99=${fmtMs(group.p99)} max=${fmtMs(group.max)}</title></polyline>`;
  });
  return svg + '</svg>';
}

function scatterSvg(points, colors, threshold) {
  const width = 720;
  const height = 300;
  const margin = { left: 64, right: 16, top: 12, bottom: 40 };
  const tMin = Math.min(...points.map((point) => point.start_us));
  const tMax = Math.max(tMin + 1, ...points.map((point) => point.start_us));
  const values = points.map((point) => point.dur_ms).filter((value) => value > 0);
  const vMin = Math.max(Math.min(...values), 1e-3);
  const vMax = Math.max(Math.max(...values), vMin * 10);
  const x = (time) => margin.left + ((time - tMin) / (tMax - tMin)) * (width - margin.left - margin.right);
  const y = logScale(vMin, vMax, height - margin.bottom, margin.top);
  let svg = `<svg viewBox="0 0 ${width} ${height}" width="${width}" height="${height}">`;
  for (const tick of decades(vMin, vMax)) {
    svg += `<line x1="${margin.left}" x2="${width - margin.right}" y1="${y(tick)}" y2="${y(tick)}" class="grid"/>`;
    svg += `<text x="${margin.left - 6}" y="${y(tick) + 4}" text-anchor="end">${decadeLabel(tick)}</text>`;
  }
  for (let index = 0; index <= 5; index++) {
    const time = tMin + ((tMax - tMin) * index) / 5;
    svg += `<text x="${x(time)}" y="${height - margin.bottom + 16}" text-anchor="middle">${fmtTime(time).slice(5, 16)}</text>`;
  }
  for (const point of points) {
    const color = colors.get(point.session_id) ?? OTHER;
    const radius = point.dur_ms >= threshold ? 3 : 1.8;
    svg += `<circle cx="${x(point.start_us).toFixed(1)}" cy="${y(point.dur_ms).toFixed(1)}" r="${radius}" fill="${color}" fill-opacity="0.7"><title>${escapeHtml(point.title)}</title></circle>`;
  }
  return svg + '</svg>';
}

function groupTable(groups) {
  let html = '<table><tr><th></th><th>group</th><th>n</th><th>p50</th><th>p90</th><th>p99</th><th>p99.9</th><th>max</th><th>total</th><th>errors</th></tr>';
  groups.forEach((group, index) => {
    const swatch = index < PALETTE.length ? `<span class="swatch" style="background:${PALETTE[index]}"></span>` : '';
    html += `<tr><td>${swatch}</td><td>${escapeHtml(group.name)}</td><td>${group.n}</td><td>${fmtMs(group.p50)}</td><td>${fmtMs(group.p90)}</td><td>${fmtMs(group.p99)}</td><td>${fmtMs(group.p999)}</td><td>${fmtMs(group.max)}</td><td>${fmtMs(group.total)}</td><td>${group.errors}</td></tr>`;
  });
  return html + '</table>';
}

function rowsTable(rows) {
  if (rows.length === 0) return '<p>(none)</p>';
  const columns = Object.keys(rows[0]);
  let html = '<table><tr>' + columns.map((column) => `<th>${escapeHtml(column)}</th>`).join('') + '</tr>';
  for (const row of rows) {
    html += '<tr>' + columns.map((column) => `<td>${escapeHtml(row[column])}</td>`).join('') + '</tr>';
  }
  return html + '</table>';
}

function report(db, options) {
  const out = path.resolve(options.out ?? path.join(REPO, 'output', 'telemetry', 'report.html'));
  const budget = Number(options.points ?? 5000);
  const sections = [
    { title: 'Nested tool calls (tool.execute) by tool', span: 'tool.execute', by: ['tool'], details: true },
    { title: 'Exec cells and waits', span: '%', by: ['name'], where: "s.name IN ('code_mode.cell', 'code_mode.wait', 'tool.call')" },
    { title: 'Model calls by model', span: 'model.call', by: ['model'] },
    { title: 'Model time to first stream event', span: 'model.call', by: ['model'],
      value: `json_extract(s.attrs, '$."time_to_first_event_ns"') / 1e6` },
    { title: 'Messages / Responses attempts', span: '%', by: ['name'], where: "s.name IN ('messages.attempt', 'responses.attempt')" },
    { title: 'Durability store by operation', span: 'durability.store', by: ['operation'] },
    { title: 'Frames (nc.frame)', span: 'nc.frame', by: [] },
  ];
  let body = '';
  const since = options.since ? ` since ${escapeHtml(options.since)}` : '';
  for (const section of sections) {
    const where = [options.where, section.where].filter(Boolean).join(' AND ') || undefined;
    const filter = spanFilter({ ...options, span: section.span, where });
    const value = section.value ?? 's.dur_ms';
    const rows = db.prepare(`SELECT s.id, ${groupSql(section.by)} AS grp, ${value} AS dur_ms, s.error, s.start_us,
        s.session_id, s.call_id, s.thread_name, s.thread_id, s.attrs, s.name FROM spans s
      WHERE ${filter.where} AND ${value} IS NOT NULL`).all(...filter.params);
    if (rows.length === 0) continue;
    const groups = groupStats(rows);
    const shown = groups.slice(0, PALETTE.length);
    const p90 = new Map(groups.map((group) => [group.name, group.p90]));
    let points = rows;
    if (rows.length > budget) {
      const tail = rows.filter((row) => row.dur_ms >= p90.get(row.grp)).sort((a, b) => b.dur_ms - a.dur_ms)
        .slice(0, budget);
      const rest = rows.filter((row) => row.dur_ms < p90.get(row.grp));
      const room = Math.max(0, budget - tail.length);
      const stride = rest.length / Math.max(room, 1);
      const sample = Array.from({ length: Math.min(room, rest.length) }, (_, index) => rest[Math.floor(index * stride)]);
      points = tail.concat(sample);
    }
    const sessionCounts = new Map();
    for (const row of rows) sessionCounts.set(row.session_id, (sessionCounts.get(row.session_id) ?? 0) + 1);
    const topSessions = [...sessionCounts.entries()].filter(([id]) => id).sort((a, b) => b[1] - a[1])
      .slice(0, PALETTE.length);
    const colors = new Map(topSessions.map(([id], index) => [id, PALETTE[index]]));
    for (const point of points) {
      const detail = section.details ? ' — ' + shorten(detailFor(db, point), 160, {}) : '';
      point.title = `[${point.id}] ${point.grp} ${fmtMs(point.dur_ms)} at ${fmtTime(point.start_us)} session ${point.session_id ?? '-'} thread ${point.thread_name}:${point.thread_id}${detail}`;
    }
    const legend = topSessions.map(([id, count], index) =>
      `<span class="swatch" style="background:${PALETTE[index]}"></span>${escapeHtml(shortSession(id))} (${count})`).join(' ');
    const sampled = points.length < rows.length
      ? `showing ${points.length} of ${rows.length} spans: the slowest spans at or above their group p90 first, then an even sample of the rest (--points)`
      : `all ${rows.length} spans`;
    const threshold = Math.min(...shown.map((group) => group.p90));
    body += `<section><h2>${escapeHtml(section.title)}${since}</h2>${groupTable(groups.slice(0, 40))}
      <h3>Tail: fraction of spans at least as slow as x (log-log)</h3>${ccdfSvg(shown)}
      <h3>Latency over time by session (${sampled}; larger dots are at or above p90)</h3>${scatterSvg(points, colors, threshold)}
      <p class="legend">sessions: ${legend} <span class="swatch" style="background:${OTHER}"></span>other</p></section>`;
  }
  const outlierSections = [['tool.execute', ['tool']], ['code_mode.cell', []], ['model.call', ['model']]];
  for (const [span, by] of outlierSections) {
    const { found } = findOutliers(db, { ...options, span, by: by.length ? by.join(',') : 'none' });
    const rows = found.slice(0, Number(options.limit ?? 50))
      .map((entry) => outlierRow(db, entry, { full: true, json: true }));
    body += `<section><h2>Outliers: ${escapeHtml(span)} above p${options.above ?? 99} of their group</h2>${rowsTable(rows)}</section>`;
  }
  const sessionRows = db.prepare(`SELECT session_id AS session, model, origin, depth, parent_session_id AS parent,
      datetime(first_us / 1e6, 'unixepoch', 'localtime') AS first, datetime(last_us / 1e6, 'unixepoch', 'localtime') AS last,
      turns, model_calls, round(model_ms / 1000, 1) AS model_s, cells, round(cell_ms / 1000, 1) AS cell_s, tool_calls,
      round(tool_ms / 1000, 1) AS tool_s FROM sessions ORDER BY last_us DESC LIMIT 100`).all();
  body += `<section><h2>Sessions</h2>${rowsTable(sessionRows)}</section>`;
  const html = `<!doctype html><meta charset="utf-8"><title>nanocode telemetry</title>
<style>
body{font:13px -apple-system,system-ui,sans-serif;margin:24px;color:#222;max-width:1200px}
h2{margin-top:36px;border-bottom:1px solid #ddd}h3{font-size:13px;color:#555;margin:18px 0 4px}
table{border-collapse:collapse;margin:8px 0;font-variant-numeric:tabular-nums}
td,th{padding:2px 8px;border-bottom:1px solid #eee;text-align:left;vertical-align:top;max-width:520px;overflow-wrap:anywhere}
svg text{font-size:11px;fill:#555}.grid{stroke:#e6e6e6}.swatch{display:inline-block;width:10px;height:10px;margin:0 4px 0 8px;border-radius:2px}
.legend{color:#555}
</style>
<h1>nanocode telemetry</h1><p>Generated ${escapeHtml(fmtTime(Date.now() * 1000))} from ${escapeHtml(homeDir(options))}${since}. Hover points and lines for details; ids in brackets work with <code>node scripts/otel.mjs trace &lt;id&gt;</code>.</p>
${body}`;
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, html);
  console.log(out);
}

const options = parseArgs(process.argv.slice(2));
const command = options._[0];
if (!command || options.help || command === 'help') {
  console.log(HELP);
  process.exit(command ? 0 : 1);
}
const commands = { ingest: null, schema, sql, tails, outliers, trace, sessions, builds, breakdown, prune, report };
if (!(command in commands)) fail('unknown command ' + command + '\n\n' + HELP);
const db = openDb(path.resolve(options.db ?? path.join(REPO, 'output', 'telemetry', 'telemetry.sqlite')));
if (command === 'ingest' || !options['no-ingest']) {
  const result = ingest(db, homeDir(options), false);
  if (command === 'ingest') {
    const totals = db.prepare('SELECT count(*) AS spans, min(start_us) AS first, max(end_us) AS last FROM spans').get();
    const range = totals.spans ? ` from ${fmtTime(totals.first)} to ${fmtTime(totals.last)}` : '';
    console.log(`${result.spans} new spans, ${result.metrics} new metric points; cache holds ${totals.spans} spans${range}`);
  }
}
if (commands[command]) commands[command](db, options);
db.close();
