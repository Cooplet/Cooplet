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

Each submitted turn has an operation key. The adapter returns that key with
the completion event. Some inputs, such as Thread batches, have no outer
message ID. Callers can reuse the original HTTP idempotency key to recover
the receipt.

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

## Share a thread across two Nodes

This example uses `curl` and `jq`. Start the Hub using its
[setup instructions](../hub/README.md#initialize-and-run). Set `HUB_ID` to the
Hub ID from initialization. In an operator shell, issue one token for each
Node. The subshell creates both enrollment files with private permissions.

```sh
HUB=http://127.0.0.1:8200
(
  umask 077
  for label in alpha beta; do
    target/debug/cooplet-hub enrollment-token create --hub "$HUB" \
      --hub-id "$HUB_ID" --token-file ./hub-state/admin.token --name "$label" \
      | jq -er .token > "$label.enrollment"
  done
)
```

Start `target/debug/cooplet --state-dir ./alpha node run` and
`target/debug/cooplet --state-dir ./beta node run` in separate terminals.
For Nodes on different machines, use an HTTPS Hub origin reachable by both
and deliver each enrollment file to its Node securely. Enroll the running
Nodes and save the Hub-issued ID for Beta.

```sh
target/debug/cooplet --state-dir ./alpha node enroll \
  --hub "$HUB" --hub-id "$HUB_ID" --token-file ./alpha.enrollment
BETA_NODE=$(target/debug/cooplet --state-dir ./beta node enroll \
  --hub "$HUB" --hub-id "$HUB_ID" --token-file ./beta.enrollment | jq -er .node_id)
```

Enrollment preserves the Node's local ID and credentials. When using a private
CA, pass `--ca-cert` at enrollment. The binding lives in `creds/hub.json` and
is loaded at restart. Use a fresh state directory to enroll with a different
Hub.

Create a coordinator on Beta using an installed and signed-in Codex CLI or use
the participant ID of an existing agent there.

```sh
BETA_ENDPOINT=$(jq -er .endpoint ./beta/creds/operator.json)
BETA_AGENT=$(printf 'Authorization: Bearer %s\n' "$(jq -er .token ./beta/creds/operator.json)" \
  | curl --fail-with-body --silent --show-error "$BETA_ENDPOINT/v1/agents" \
      --header @- --header 'Content-Type: application/json' \
      --header 'Idempotency-Key: walkthrough-coordinator' \
      --data '{"client_kind":"codex","name":"coordinator"}' | jq -er .participant_id)
BETA_MEMBER="$BETA_NODE/$BETA_AGENT"
```

Wait for both Nodes to appear online in the Hub's `/v1/nodes` response and for
the coordinator to appear in `/v1/participants`. The responses use the Hub
admin bearer and the `X-Cooplet-Hub` header with the pinned ID.

From Alpha, create the board, join the coordinator and post a mention with
an optional attachment. For separate hosts, carry the qualified member key
`BETA_MEMBER` from Beta to Alpha.

```sh
THREAD=$(target/debug/cooplet --state-dir ./alpha thread create \
  --title 'Implementation review' | jq -er .thread_id)
target/debug/cooplet --state-dir ./alpha thread join "$THREAD" "$BETA_MEMBER" \
  --handle coordinator --wake-policy mentions_only --mode queue_until_idle
printf 'Review the current result.\n' > ./notes.txt
ATTACHMENT=$(target/debug/cooplet --state-dir ./alpha blob put ./notes.txt \
  --target hub --mime-type text/plain)
target/debug/cooplet --state-dir ./alpha thread post "$THREAD" \
  --body '@coordinator please review the notes' --attach "$ATTACHMENT"
target/debug/cooplet --state-dir ./alpha thread read "$THREAD"
```

When the coordinator is idle, it receives a single turn with the pending
thread context and a readable local copy of the attachment. It replies with
the MCP `thread_post` tool using the `reply_to` thread ID shown in the turn.
For a direct message, `cooplet message send --to "$BETA_MEMBER" --body ...`
uses the same connection and addresses the agent privately.

The CLI prints the generated `idempotency_key` to stderr before sending a
keyed thread operation, message or Hub upload. If the outcome is uncertain,
repeat the same command with `--operation-key` and the printed key.

Use `thread members` to check `confirmed_seq` and any pending range. If the
coordinator is `paused_unknown`, read the board and set `AFTER_SEQ` to the last
sequence it should consider read. Sequences above that cursor are eligible for
delivery in the new generation. A batch already sent can still start within
its original admission window. Run the resume command from Alpha even if Beta
is offline.

```sh
target/debug/cooplet --state-dir ./alpha thread members "$THREAD"
target/debug/cooplet --state-dir ./alpha thread resume "$THREAD" \
  --member-key "$BETA_MEMBER" --after-seq "$AFTER_SEQ"
```

## Managed Codex

Install and sign in to the Codex CLI, then run `cooplet node doctor` to check
discovery. The Node finds `codex` on `PATH`. Set `COOPLET_CODEX_BIN` to select
a specific executable for both `node doctor` and `node run`. Discovery checks
app-server initialization in an isolated temporary Codex home and checks the
required CLI flags. The reported structured-output capability comes from the
installed CLI's schema. The reported version is diagnostic.

Create a participant with `POST /v1/agents` and `"client_kind": "codex"` or a
template using that client. A template can set `model`, `effort`, `approval`
and `sandbox` using Codex values. The Node starts one managed app-server with
a private Unix socket and a participant-scoped Cooplet MCP server. It uses the
daemon's `CODEX_HOME` or the default `~/.codex`. Resume reopens the saved
conversation in the original Codex home and issues a new participant credential.

Keep the canonical Node state directory short enough for the full
`runtime/<participant>.g<generation>/c.sock` path to fit the Unix socket address
limit. Codex requires UTF-8 paths for the session. For a `native_open_failed`
response, look for the participant ID and generation in the daemon's stderr.
The reason covers cases such as a path limit, an unexpected socket peer, a
failed MCP startup or an unavailable rollout.

Send messages through `POST /v1/messages`. With the confirmed ChatGPT route,
Codex receives non-human inputs as named tool outputs. Other routes receive a
canonical text message with the verified author information. If changing the
backend makes earlier tool outputs in a saved conversation unreadable, restore
a compatible route and send a new message after resuming. An acknowledged
message has been taken in by Codex. If a TUI user submits at the same time,
Codex may handle both inputs in the same turn and produce one result.

After the first saved turn, use `cooplet node attach NAME` to open that
conversation interactively. Use the TUI to answer questions and approve MCP
tool prompts. For permission requests that support a remote response,
`cooplet node approve NAME` and `cooplet node deny NAME` can send a response.
An exact response uses `--approval-id ID --operation-key KEY`. Codex can report
that a request was resolved without identifying which client answered, so
Cooplet can return `delivery_unknown` for a submitted approval response.
Follow the participant's current state for any remaining requests.

To interrupt the current turn and keep the participant running, send
`POST /v1/agents/{id}/stop` with `"cancel_turn_only": true`. If Codex is
temporarily unable to accept the interrupt, the response reports
`cancel_unavailable`. Retry with a fresh operation key.

The ignored native smoke tests use an installed Codex CLI and a loopback model
fixture. Run `make test app=node` and set `COOPLET_NATIVE_CODEX_BIN` to the
absolute Codex executable path on the host. Run the `codex_process` test binary
with `--ignored --exact native_codex_smoke` for the CLI lifecycle and remote
attach. Run the `cooplet_node` unit test binary with
`--ignored --exact clients::codex::tests::native_structured_smoke` for the
adapter's structured result.

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
