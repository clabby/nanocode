Native shell, process-group cleanup, environment handling and PTY execution. Keep tool descriptions and handlers in each backend. Preserve nanocode-tool-runtime::shell reexports. This adapter has no dependencies on WASM.
Keep native async implementation futures erased at crate boundaries so callers do not inherit large concrete future types. Preserve awaitable call syntax, output contracts, cancellation on drop and caller policy. Do not narrow generic callers with new Send bounds or apply native Send requirements to WASM-local futures.
Native concrete entry points return erased futures directly. Avoid public async wrappers that expose another generator type to callers. Preserve higher-ranked observer callbacks and generic transports with their existing conditional Send behavior.

