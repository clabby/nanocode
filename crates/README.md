# Crate layout

| Directory | Responsibility | Expected changes |
| --- | --- | --- |
| `nanocode/` | Public Rust composition and builders | Public API wiring |
| `runtime/` | Agent lifecycle, Code Mode, durability, and subagent orchestration | Active runtime development |
| `backends/` | Provider APIs, execution adapters, prompts, backend-owned tools, and each backend's durability adapter | Provider-specific maintenance |
| `core/` | Shared types, tool contracts, macros, media, platform interfaces, TLS, and home configuration | Changes to shared contracts or infrastructure |
| `integrations/` | Computer use, observability, and state stores | Integration-specific development |
| `tui/` | Reusable terminal UI components (`nanocode-tui`): terminal session, composer, transcript, markdown, diff, pickers, queue, agent tree | Terminal presentation |
| `../bin/nanocode/` | The `nanocode` / `nc` interactive agent built on `nanocode` and `nanocode-tui` | Application behavior |

Each backend is one crate containing its API definitions, tools, and durability
adapter. `core/` depends on nothing above it; `runtime/` depends only on `core/`
and `runtime/`. `scripts/check-dependency-boundaries.mjs` enforces both.

Directory placement describes ownership. Cargo dependencies determine rebuilds;
keep backend details out of shared contracts and runtime-only changes out of backends.

`nanocode-code-mode-engine` owns the native QuickJS execution worker and its
event protocol. It depends only on core tool contracts and the execution engine,
so Cargo can compile it alongside MCP rather than inside the Code Mode facade.
`nanocode-code-mode` preserves the existing entry points and orchestrates both.
The native worker is gated out on WASM, which continues to use the Rust host
interface for Code Mode execution.

## Durability

`nanocode-durability` owns the durable state machine, the `StateStore` contract,
`MemoryStore`, the session catalog, and the `CheckpointFormat` trait. It has no
backend or database dependencies and builds for native and WASM.

Each backend implements `DurableAgentExt`, `DurableBackendRegistration`, and a
`CheckpointFormat` in its own `durability` module. Third-party backends use the
same traits. `SessionStore::register_format` registers a format on every clone of
the store; the composed runtime registers the Codex, Claude, and configured backend
formats on the store passed to `.durability(...)`, which accepts any `StateStore`.

Durability is enabled by default on the composed runtime. Native sessions use
`nanocode-store-jsonl` at the configured Codex home under `nanocode/sessions.jsonl`.
`.durability(...)` selects one shared store for roots and their subagents.
`nanocode-store-sqlite` and `nanocode-store-postgres` are optional store crates.
The JSONL adapter keeps a rebuildable SQLite offset index beside each log as
`<log>.index.sqlite`. Payloads and immutable records remain in JSONL; opening an
indexed log or resuming one state reads only its index pages and referenced log
entries. The first open builds the index for existing logs. JSONL is synced
before the index advances, and any unindexed tail is replayed on recovery.
Index transactions and connection shutdown share the log's file lock. The index
uses SQLite WAL with normal synchronization and requires a local filesystem;
its offsets use SQLite's signed 64-bit integer range, while revisions and fences
retain the full unsigned 64-bit range. Removing the index rebuilds it from JSONL.
WASM defaults to memory; hosts implement `StateStore` over persistent storage to
retain sessions across host restarts.

Beginning a previously started step without a committed receipt returns
`BeginStep::Interrupted`. The built-in backends record an unknown-outcome error
for interrupted tool calls and deliver it to the model without invoking the
handler again. Committed tool receipts replay their recorded output.

## Composition

| Host | Crates |
| --- | --- |
| Native TUI or CLI | `nanocode`, `nanocode-tui`, a store such as `nanocode-store-jsonl` or `nanocode-store-sqlite`, optionally `nanocode-observability` |
| Native server | `nanocode`, `nanocode-store-postgres`, `nanocode-observability` |
| WASM worker | `nanocode` and a host `StateStore` |

`nc` records OpenTelemetry traces and metrics by default in its home directory
under `log/otel/<timestamp>-<pid>/traces.jsonl` and `metrics.jsonl`. Each line is
an OTLP JSON export request with resource and scope metadata. Completed spans
are written immediately; metrics are written periodically and at shutdown.
`--home` also selects this telemetry location. `--otel-endpoint` additionally
exports to an OTLP/HTTP collector, while local recording continues.
`ObservabilityBuilder::otlp_directory` enables the same file exporters in native
hosts without changing library defaults.

TUI traces include `nc.input`, `nc.command`, `nc.action`, `nc.app_event` and
`nc.frame`. Frame spans link to the input that requested them and carry session
IDs and input/stream/resize causes. Nested spans measure transcript cache misses
and visited cells, markdown/code/diff formatting, composer and overlay rendering,
pane rendering, terminal diffing, encoding and writes, plus session loading,
replay and restoration. Frame metrics report input/stream/resize latency and
frame duration. Action spans measure dispatch work; asynchronous model and tool
work has its own spans. Export uses completed spans, so active operations appear
when they finish.

## WASM compilation

This is one Rust crate workspace. Reusable crates compile individually for native
and `wasm32-unknown-unknown` targets. There is no separately packaged WASM SDK,
JavaScript package, or project-owned wasm-bindgen ABI. Language bindings belong
in an external repository.

`nanocode-platform` provides the Rust host interface used by portable crates.
On native targets it uses the operating system and Tokio. A WASM embedding
installs one `Rc<dyn nanocode_platform::Host>` per execution thread before using
the runtime. The host schedules local futures, implements cancellable sleeps,
supplies monotonic and wall clocks, and fills buffers with cryptographically
secure random bytes. The host is installed once for that execution thread. Dropping a sleep
future must cancel its timer. UUIDv7 uses the RFC 9562 48-bit Unix millisecond timestamp;
clock values outside that range are rejected.

Codex networking uses `nanocode::codex::transport::host::HostTransport`.
Claude networking uses `nanocode::claude::transport::HttpTransport` and its
streaming response body. WASM hosts also supply tool execution and persistent
storage through the existing Rust contracts. Native terminal, filesystem,
database, desktop-process, and telemetry exporters are native adapters, gated out
on WASM. Their library targets compile without exposing native APIs on WASM;
the `nc` and `nanocode` executables run on native targets.
`nanocode-schema` validates output contracts without JavaScript bindings or
host randomness. The active WASM dependency graph contains no `wasm-bindgen`.
Cargo.lock can still list optional JavaScript dependencies of native-only upstream
packages; those packages are excluded from WASM builds.

Check a reusable crate with `cargo check -p <crate>` and
`cargo check --target wasm32-unknown-unknown -p <crate>`.

Check every library target with `cargo check --workspace --lib` and
`cargo check --workspace --lib --target wasm32-unknown-unknown`.

## Focused dependency replacements

Output-contract validation uses [`nanocode-schema`](core/schema/README.md), a
locally maintained adaptation of Boon with full draft 4 through 2020-12 support.
It replaces the broad jsonschema dependency tree while keeping schema checking,
references, format behavior and validation error locations. Its sources retain
upstream licenses and provenance. No project Cargo features are needed.

The MCP client uses [`nanocode-mcp`](core/mcp/README.md) rather than rmcp and
oauth2. The portable crate provides JSON-RPC and protocol types, the client
session, SSE parsing and OAuth helpers. `nanocode-mcp-http` holds the native
Streamable HTTP transport and the OAuth discovery, login and refresh flows; it
compiles to an empty library on WASM. Code Mode keeps the stdio transport and
the tool and runtime orchestration, and re-exports the public OAuth types. Stdio, Streamable HTTP with
SSE, session re-initialization, paginated discovery, payments, OAuth login and
refresh keep their existing behavior.

MCP tool discovery uses an internal immutable BM25 index rather than the bm25
crate. Tokenization and scoring parameters are preserved; ties are deterministic.
See [the scoring contract](runtime/code-mode/src/mcp/SEARCH.md).

Claude's legacy subscription user ID uses the pinned Claude Code wire format:
`user_<64 ASCII hex digits>_account_<lowercase hyphenated UUID>_session_<lowercase hyphenated UUID>`.
It is parsed directly with the existing UUID dependency. General regex support
is confined to native tools. Codex enables only Tower retry and service utilities;
the TUI loads built-in syntax definitions and supplies its own highlighting theme.
