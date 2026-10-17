Own backend-independent runtime assembly, agent ownership, durable recovery and subagent coordination. Receive provider construction and host services through interfaces. Keep substantial assembly outside generic public builders, and keep facade compatibility methods as thin forwards. Do not depend on backend or integration crates.

AgentRuntime::open_loaded consumes an already loaded session through the configured store, preserving validation and ownership acquisition while avoiding another catalog load. SessionStore::prepare_resume_loaded validates its original store and identity; any backend preload must match the acquired checkpoint before reuse.

Validate WASM workspace paths with Path::has_root: wasm32-unknown-unknown uses root-based virtual paths, while std Path::is_absolute requires a prefix on that target and rejects /workspace. Keep relative paths invalid.

RuntimeBuilder::build_async batches native runtime construction onto a dedicated thread, including default storage opening and workspace resolution; keep build as the synchronous compatibility entry point. Cancelled construction must not keep Tokio runtime shutdown waiting on a journal lock. WASM construction remains local. Runtime shutdown awaits store flush after host and owned-agent shutdown, including their error paths, so native adapter caches can finish without blocking Drop.
