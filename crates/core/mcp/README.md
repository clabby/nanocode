# nanocode-mcp

Portable Model Context Protocol client contracts used by Code Mode. It replaces
the rmcp 3.0.0 SDK with only the client surface Nanocode exercises, and builds
for native targets and `wasm32-unknown-unknown` without host dependencies.

| Module | Contents |
| --- | --- |
| `jsonrpc` | JSON-RPC 2.0 messages, request IDs and error objects |
| `protocol` | Initialize, tools, resources and call result types for protocol `2025-11-25` |
| `session` | Transport-independent client session: request correlation, initialize handshake, server ping/roots/elicitation/sampling replies, cancellation |
| `sse` | Incremental Server-Sent Events parser |
| `oauth` | OAuth discovery URLs, metadata and resource validation, scope selection, PKCE, token request bodies and responses |

A transport drains `Session::new`'s `Outgoing` receiver and delivers server
messages through `Inbound`. Dropping every `Session` clone closes the
receiver, which tells the transport to shut down. Dropping a pending request
other than `initialize` sends `notifications/cancelled`, and a server
cancellation fails the matching request. The native Streamable HTTP transport and the
OAuth HTTP flows live in `nanocode-mcp-http`; the stdio transport lives in
`nanocode-code-mode` under `src/mcp/`.

Content blocks, resources, resource templates and resource contents keep the
server's JSON objects. Text, image and audio blocks are inspected through
`ContentBlock::kind`.

## Limits

These values match rmcp 3.0.0 and oauth2 5.0.0 behavior, or come from the
protocol or RFC:

| Limit | Value | Source |
| --- | --- | --- |
| Protocol version | `2025-11-25` | rmcp `ProtocolVersion::LATEST` and the MCP 2025-11-25 lifecycle |
| Discovery `MCP-Protocol-Version` | `2024-11-05` | rmcp OAuth discovery requests |
| PKCE verifier | 32 random bytes, base64url | oauth2 `PkceCodeChallenge::new_random_sha256`, RFC 7636 |
| OAuth state | 16 random bytes, base64url | oauth2 `CsrfToken::new_random` |
| SSE event size, set by the caller | 16 MiB of retained event data | rmcp `DEFAULT_MAX_SSE_EVENT_SIZE` |
| Stdio message size | 8 MiB per line | Existing Code Mode stdio transport |
| OAuth HTTP response body | 1 MiB | rmcp `MAX_OAUTH_HTTP_RESPONSE_BODY_BYTES` |
| Session DELETE on shutdown | 5 seconds | rmcp `SESSION_CLEANUP_TIMEOUT` |
| Response stream drain after the final response | 50 ms | rmcp streamable HTTP worker |
| SSE reconnect | Server `retry`, otherwise 1 second doubled per failed attempt | rmcp `ExponentialBackoff` defaults and the MCP Streamable HTTP resumability rules |
