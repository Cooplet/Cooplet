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
the database with a read-only connection. It also reports client availability
and the workspace it inspects for agent templates. The workspace defaults to
the current directory. Pass `cooplet node doctor --workspace-dir PATH` to
inspect another workspace.

A registered adapter can provision agents from
`.cooplet/agents/<template-id>/agent.json` in the workspace, with a fallback
under `$XDG_CONFIG_HOME/cooplet/agents/` or `~/.config/cooplet/agents/`. Put
`"schema": 1` and the registered `"client"` kind in `agent.json`. An optional
`instructions.md` supplies instructions. Node freezes those instructions when
it creates the participant and reuses them on resume.

## Managed Pi participants

Install [Pi](https://pi.dev/) on the Node's `PATH` and configure a model in Pi.
`cooplet node doctor` reports whether the installed CLI advertises the required
RPC options. Node uses Pi's native RPC mode and verifies the session and active
Cooplet MCP tools before exposing the participant as ready.

Send `{"client_kind":"pi","name":"reviewer"}` to `POST /v1/agents` to create
a participant. A template can set `"client":"pi"`. The `model` option uses
Pi's model selector, and `effort` uses Pi's `--thinking` levels. Explicit
`approval` and `sandbox` options are unsupported.

Use `POST /v1/messages` to deliver a turn, and
`POST /v1/agents/{id}/stop` and `POST /v1/agents/{id}/resume` to stop and
reopen the same conversation. Set `"cancel_turn_only":true` on stop to
cancel the current run and keep the session open. Pi stores native JSONL in
`cooplet-sessions/` under `$PI_CODING_AGENT_DIR` or `~/.pi/agent`. Preserve
that directory alongside Node state for cold resume. Cooplet reports TUI
attachment as unsupported for both running and stopped Pi participants.

A delivered receipt confirms Pi intake. Pi saves the assistant turn in its
native history. To send a reply through Cooplet, the participant calls the
Cooplet MCP `send_message` tool.

A Pi extension can request another model response before the current turn
settles. Cooplet waits for settlement and uses the final assistant response.
If a Pi extension starts overlapping native runs and Pi's events cannot
identify their completion, Node marks the participant unknown. Stop and
resume the participant to reopen the conversation.
During manual Pi compaction, the participant is busy. Queued messages
become eligible when compaction ends.

Run `python3 apps/node/tests/pi_live.py` after building Node to exercise the
production registry, native Pi and participant MCP against a local model
fixture. The test accepts `--pi PATH` and `--cooplet PATH`.

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
