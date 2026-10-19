# Nanocode workspace runtime

`workspace_runtime::WorkspaceToolRuntime` executes `exec_command`, `write_stdin`,
`apply_patch`, and `view_image` through the same handlers as the native tool
registry. Interactive shell sessions remain alive for the lifetime of the
runtime. `WorkspaceToolRuntimeControl` terminates their process groups and
descendants.

The Codex backend owns these tool definitions and handlers. The host adapters in
`nanocode-tool-runtime` provide registration, process execution, MCP and remote
transport. `nanocode-code-mode` provides generic exec/wait orchestration.
Native/WASM target gates select the host implementation without Cargo features.

`standard` contains tool identities and definitions. Shared model-visible tool
contracts live in `nanocode-tools` and wire types in `nanocode-types`.

## Remote workspace runners

The native SDK owns a shared `RunnerPool` for each `Nanocode` runtime. Configure
outgoing runners with `.runner(Runner::connect(url).token(token).name("box").alongside())`
on the builder, or call `runtime.connect_runner(...)` on a live runtime. Use
`.runner_server(RunnerServerConfig::listen(address).token(token))` at startup or
`runtime.listen_for_runners(...)` at runtime to accept multiple incoming runners.
Incoming catalogs supply their names and workspaces; names must be unique among
active connections. An omitted listener token is generated from 256 random bits.
Explicit disconnect revokes that executor's runtime identity and stops automatic
reconnection. A fresh runner process may attach again under the same name.
`RunnerHandle` exposes status subscriptions and disconnect; `RunnerServer` exposes
its endpoint, token, and stop operation. `list_runners` and `runner_events` expose
runtime-wide status. The pool is shared by all built-in agents and subagents.

Dynamic attachment leaves model-visible tool definitions and system instructions
unchanged. Conversation developer messages announce mount availability. Code Mode
execution metadata discovers the currently attached tools on each cell. A detached
mount remains reserved; workspace calls record known mount names alongside durable
process routes. Resuming without the runner produces an unavailable error instead
of interpreting its logical root as a local path. Replacement is explicit and
remains unavailable while its runner is disconnected.

A static Code Mode tool named `runners` lets the model list, connect, listen,
disconnect, or stop listening when requested. List and connect results omit
credentials; listen returns the credential required to start an incoming runner.
Hosts can disable this tool with `NanocodeBuilder::runner_management(false)` or
`RunnerPool::with_model_management(false)` before constructing their tools.

When composing Code Mode directly, install a shared pool with
`Tools::with_runner_pool` or `ToolsBuilder::runner_pool`. Existing fixed-catalog
attachment APIs remain available below.

`attachment::run_runner(RunnerConfig { endpoint, token, workspace, name })`
runs a standalone native workspace executor. `RunnerEndpoint::Listen(address)`
accepts authenticated agent connections on a loopback WebSocket listener;
`RunnerEndpoint::Connect(url)` connects to an agent's authenticated `RunnerHost`.
Use a TLS proxy and `wss` when exposing a runner beyond loopback.
`run_runner_with_tools(config, tools)` also exports caller-supplied host adapters.

An agent connects with `RemoteRunner::new(name)` and
`runner.connect(AttachmentTarget::new(url, token)?)`. Keep the returned
`RunnerConnection` alive. For incoming runners, bind `RunnerHost`, register
`host.runner(name, token)`, and keep the host alive. Obtain the published catalog
with `runner.tools().await`, then configure
`Tools::builder().remote_runner(catalog, physical_workspace, mode)?.build()`.
Pass those tools to `CodexHarness::tools` or `ClaudeHarness::tools`.
Standalone runners advertise their canonical physical workspace in the catalog.
`RemoteTools::workspace` exposes it, and
`ToolsBuilder::remote_runner_discovered(catalog, mode)` uses it without an
attach-side path guess. Explicit workspace overrides remain available for
adapters without machine metadata. A reconnect cannot change the pinned
workspace silently.

`RemoteMode::Alongside` keeps the default cwd local. `/<name>` selects the
runner's physical workspace, and `/<name>/src` selects its `src` directory.
Multiple alongside runners may be registered. `RemoteMode::Replace` sends
workspace tool calls to its sole runner by default. Tool descriptions advertise
these mounts. Commands and file paths select their runner by mount; process
session IDs retain the selected runner for subsequent `write_stdin` calls.
Patch file headers must select one machine for the entire patch. Custom
function adapters receive an optional `workdir` routing field; routing consumes
that field before forwarding their own arguments.

Runner catalogs also contribute dynamic MCP tools to this shared namespace.
The same tool name selects a local provider by default in alongside mode or its
remote provider through `workdir`. Provider arguments do not receive this routing
field. CUA `mcp__cua_repl__js` and `mcp__cua_repl__reset` always require an explicit
`workdir` selecting a runner with a desktop provider. Calling `js` with only
`workdir` returns that desktop's tool contract without executing it. A root
without a desktop returns an error.

Logical process handles are partitioned by agent session ID and retain their
runner namespace and executor runtime identity. `ProcessRouteStore` supplies
`load` and `save` for these mappings; configure it through
`ToolsBuilder::process_route_store` or `Tools::with_process_route_store` when
composing Code Mode directly. The native SDK supplies an adapter using its
configured durable store. Reopening a durable session can poll its original
remote handles while that runner remains alive. A restarted runner or a local
process from a previous SDK runtime is fenced explicitly.
`Tools::with_default_process_route_store` fills an unset store while preserving
an explicit caller store; SDK default adapter installation uses this hook.

Reconnections recover existing calls from the same executor runtime's journal.
If the executor restarts, sent calls return an unknown-outcome error instead of
being replayed. New calls resume on the replacement connection. Cancellation
and turn-end frames are retained across connection gaps. A catalog timeout equal
to JavaScript's maximum safe integer denotes the existing unbounded timeout;
its wire deadline is that maximum timestamp. The SDK unbounded output token budget maps to the same safe-integer sentinel,
and zero output budgets remain zero. Other timeouts and budgets must
fit the wire protocol's integer range.

`RemoteRunner::with_call_timeout(Duration)` sets a caller policy applied to every
call in addition to the catalog deadline; the earlier deadline wins. Omitting
the policy preserves the catalog deadline. Durations must be positive whole
milliseconds within the protocol integer range. Expiration returns
`AttachmentError::Timeout` with the call ID and its outcome: `Ambiguous` after
dispatch or `Unavailable` before dispatch. Dispatched calls may have executed
and must not be retried automatically.
The same explicit policy bounds initial catalog readiness. Without a ready
runner, replace-mode startup waits for readiness or returns an unavailable
error; configured calls remain remote and never use embedded local execution.
Standalone runner stderr reports connecting, connected, retries, authentication
failures, and tool dispatch/completion without printing credentials.

These socket entry points are native-only. WASM hosts continue to supply their
transport and local execution through the embedded Code Mode host contract.
