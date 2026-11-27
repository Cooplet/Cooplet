# Cooplet Node

The Node runs in the foreground and serves its API on IPv4 loopback. Build it
with the workspace toolchain described in the [repository README](../../README.md).

```sh
make setup
make build app=node
target/debug/cooplet node run
```

On first start the daemon prints a JSON record with its Node ID, loopback
endpoint, operator participant ID, and private operator credential path. The
record provides the canonical path as a string in `operator_credential`.
If the path contains non-UTF-8 bytes, that field is `null` and
`operator_credential_bytes` contains the exact Unix path as an array of bytes.
The Node keeps this identity and endpoint across restarts. Use `--port PORT` on
first launch to choose the port. The state directory defaults to
`$XDG_STATE_HOME/cooplet` or `~/.local/state/cooplet`. A separate `--state-dir`
creates an independent Node. Keep the database, credential files, and blob
directory together when restoring a Node.

In another terminal, using the same state directory:

```sh
target/debug/cooplet node status
target/debug/cooplet node doctor
target/debug/cooplet watch --after 0
target/debug/cooplet blob put ./notes.txt --mime-type text/plain
target/debug/cooplet blob get BLOB_ID --output ./downloaded.txt
target/debug/cooplet node stop
```

`watch` prints NDJSON frames. If the cursor has fallen outside the retained
event window, the CLI reports the available bounds and exits with an error.
Use a fresh snapshot and continue with a cursor in the current window.

The [Node OpenAPI contract](../../contracts/openapi-node.json) describes the
loopback API. Use the private bearer descriptor for authenticated requests.
Send the same `Idempotency-Key` to recover a receipt for the same mutation.
A `delivery_unknown` response means that Node could have applied the external
effect. A retry with the same key returns the recorded outcome without
resubmitting it.
If an interrupted agent operation leaves an agent in `starting` or `stopping`,
an operator can continue cleanup with a fresh key on
`POST /v1/agents/{id}/stop`. A `cleanup_unverified` result keeps the agent
blocked until cleanup is confirmed.
Reusing a `node stop` key prints the original receipt. Use a fresh key to stop
the currently running daemon.

When the Node is stopped, `node doctor` checks the saved credentials against
the database with a read-only connection. The report identifies the workspace
it inspects for agent templates. It defaults to the current directory. Pass
`cooplet node doctor --workspace-dir PATH` to inspect another workspace.

A registered adapter can provision agents from
`.cooplet/agents/<template-id>/agent.json` in the workspace, with a fallback
under `$XDG_CONFIG_HOME/cooplet/agents/` or `~/.config/cooplet/agents/`. Put
`"schema": 1` and the registered `"client"` kind in `agent.json`. An optional
`instructions.md` supplies instructions.

For message delivery, the adapter receives the message ID as its turn
correlation key. Echo that key in completion events when available. Callers
can independently reuse the same HTTP idempotency key.

Each attachment in the adapter's message envelope includes its blob ID, name,
and absolute `local_path`. A non-UTF-8 path is encoded as `local_path: null`
with the exact Unix bytes in `local_path_bytes`. A message accepts up to 64
unique attachment blob IDs.

On restart, startup crash recovery revokes credentials for any agent that was
not stopped cleanly and removes revoked credential files and orphaned runtime
directories while preserving `creds/operator.json`.

Adapters start the MCP server with
`cooplet node mcp --participant-cred PATH`, using the private descriptor for
the current participant generation. The MCP server uses that identity for
messages, questions, and attachments. MCP file uploads require a regular
workspace file with a single hard link so its contents can be attributed to
the permitted workspace. Operator approval and attach commands
use the local operator credential.

`cooplet node attach NAME` uses the active participant with that name or its
sole stopped match. Pass the exact participant ID when several stopped
participants share a name.

## Antigravity CLI (Agy)

Install `agy` on the Node's `PATH` and sign in with the CLI as the user who
runs the Node. `cooplet node doctor` probes ACP v1 support and reports whether
the CLI supports reopening conversations and appending session instructions.
Use `"client_kind": "agy"` in an agent create request or `"client": "agy"` in a
template. Optional `model` and `effort` overrides select values advertised by
Agy. The Node saves the native session ID and store location
(`$HOME/.gemini/antigravity-cli` by default) so a stopped agent can resume the
same conversation. Each open uses its own participant MCP credential and
appends the instructions frozen at creation to Agy's native system prompt.

Agy runs with full access inside the trusted workspace. The accepted
`approval` override is `never`, and `sandbox` may be `none` or
`danger-full-access`. Both are optional. The scoped MCP credential controls
the identity and permissions of MCP calls. A process running as the same OS
user can access that user's files and services. Run the Node and Agy inside an
isolated container or VM for workloads that require an OS security boundary.
Interactive approvals, steering, structured turn output, and TUI attach are
unavailable on this adapter. Use the Node stop endpoint with
`cancel_turn_only: true` to cancel a turn without stopping its process.

ACP v1 keeps the prompt request open until the turn finishes. The Node marks
delivery after the full prompt is written to the private child pipe and a
short window for immediate rejection passes. Turn completion arrives
separately. If handoff becomes uncertain, the Node records the unknown
outcome and reuses that receipt for the same idempotency key. The result
includes the assistant text streamed during the active turn. ACP v1 updates
have no turn ID, so text from a background task emitted during a later turn
can be included in that turn's result.
