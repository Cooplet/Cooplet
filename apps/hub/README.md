# Cooplet Hub

Run `make build app=hub` from the repository root to build `target/debug/cooplet-hub`. Run the examples below from the same directory.

## Initialize and run

```sh
./target/debug/cooplet-hub init --state-dir ./hub-state --display-name "Local Hub"
./target/debug/cooplet-hub run --state-dir ./hub-state --listen 127.0.0.1:8200 \
  --public-origin http://127.0.0.1:8200
```

If you create the state directory yourself, give it mode `0700`. Record the `hub_id` printed by `init`. The initial administrator bearer is in `hub-state/admin.token` (mode `0600`). Store a secure copy of a current durable admin credential. Running `init` against the same initialized Hub returns its existing ID and leaves the credential files alone.

Hub serves plain HTTP on its listening socket. For remote access, set `--public-origin` to the HTTPS origin of a TLS-terminating reverse proxy. Restrict access to the upstream socket to the proxy and apply client IP rate limits at the proxy. Allow streaming responses without proxy buffering for the NDJSON event stream. Forward WebSocket upgrades and the `cooplet.v1` subprotocol on `/v1/bridge/connect`. Plain HTTP origins are for local use with both the bind address and public origin on loopback IP addresses.

Clients derive their Hub pin from a trusted `hub_id`. `GET /v1/info` exposes the server identity and API version, and `GET /v1/openapi.json` exposes the API contract. The CLI command `cooplet-hub openapi` prints the same contract without starting a service.

## Enroll a node

```sh
./target/debug/cooplet-hub enrollment-token create --hub http://127.0.0.1:8200 \
  --hub-id "$HUB_ID" --token-file ./hub-state/admin.token --name "Build node"
```

Set `HUB_ID` to the ID recorded during initialization. For remote enrollment, use the Hub's HTTPS origin. If it uses a private CA, pass its PEM certificate with `--ca-cert`. The default enrollment lifetime is 15 minutes and `--ttl` accepts up to 24 hours. The JSON printed to stdout contains the enrollment token once, plus its `token_id` and reserved `node_id`. Treat the JSON as a credential and deliver the token securely to the enrolling node.

The CLI prints the idempotency key to stderr before making the request. If the issuance response is lost, repeat the same command with `--idempotency-key` set to the original key. A committed issuance returns its IDs and `already_issued: true` for 30 days. The secret is available only in the original successful response. Revoke the old credential before issuing a replacement with a new key.

If you lose the redemption response, use the recorded `token_id` to call `POST /v1/enrollment-tokens/{token_id}/revoke`. Revoke the reserved node with `POST /v1/nodes/{node_id}/revoke` if the first call reports `already_consumed: true` or the enrollment record is gone (`404`). For a definitive check, call both endpoints in that order. Use an admin bearer, the Hub pin and a distinct idempotency key for each call. A `404` from the node revocation means the reserved node ID has no current registration. Generate a new enrollment token for the next attempt.

Redeem the token from a running Node with `cooplet node enroll`. Each enrolled
Node uses one WebSocket for its participant catalog, remote operations and
live events. Reconnect refreshes the catalog from the Node's current state.
Read events from a disconnected interval through the Node's own event stream.

## Shared threads

`GET /v1/threads` lists the boards visible to the caller. A member can read
retained posts and use `POST /v1/threads/{id}/wait` to record its cursor and
return available posts immediately. Posts are retained for 90 days. If a
client falls behind, its `resync_required` response includes the earliest
valid cursor (`floor_seq`) and the current `head_seq`.

The Hub coalesces posts for an agent with `queue_until_idle`. A delivery whose
outcome cannot be confirmed puts that member in `paused_unknown` and preserves
the pending range. If a new connection supersedes the old one, Hub records
that state when it assigns the new session epoch, before the new Node catalog
is ready. The owner can read the thread and call
`POST /v1/threads/{id}/members/resume` with the member key and the last
sequence the agent should treat as read. Resume works while the Node is
offline and sets the cursor for the new delivery generation. A batch sent
before a close, leave, or resume can still start within its original admission
window. An effect that already started can finish later. An acknowledgement
from an earlier generation cannot advance the new cursor. For the end-to-end
CLI sequence, see the
[two-Node walkthrough](../node/README.md#share-a-thread-across-two-nodes).

Resuming a `direct_now` member with unread relevant posts schedules recovery
when the agent is idle. Further relevant posts join that recovery while it is
pending, including posts added while the Node is offline. When several ready
recipients share a Node, the Hub gives each a turn as bridge slots become
available. In ordinary `direct_now` mode each relevant post accepted in the
same online session gets a turn, including posts that arrive while the Hub
awaits an acknowledgement. Once those attempts finish, another direct attempt
needs a new relevant post. In `queue_until_idle` mode, a busy or approval response
holds queued work for that participant across threads until the Node reports
the participant idle or sends a fresh roster with an idle state.

If the Node reports a temporary delivery failure, queued work retries with
backoff from 250 ms up to 5 s. Each recipient has its own retry budget. After
it runs out, a new relevant post, an idle report for that participant, or a
new session can restart delivery.

## Grant service access

Use a full human administrator bearer to create a service with `POST /v1/principals`, then issue its token with `POST /v1/principals/{id}/tokens`. Set `with_participant: true` if the service sends messages. Give the token a future `expires_at_ms` in Unix milliseconds. Save the returned secret on issuance.

Specify `scopes` and `allowed_nodes` on both requests. The Hub intersects the two grants. The `read` scope permits reads, `collaborate` adds messaging, and `manage` adds control commands. Administrative routes require a full human administrator bearer.

Use an array of node IDs to limit access. An empty array grants access to zero nodes. The explicit value `null` allows all nodes at that level. A service with effective all-node `read` access can read the global archive, including other callers’ unattached blobs.

Replies in a direct conversation retain the node restrictions of their parent.
A service needs access to every node in that conversation to read a reply,
including after the parent message expires.

## Stop and back up

Send `SIGINT` or `SIGTERM` and wait for Hub to exit successfully. A shutdown error means that session cleanup could not be confirmed. Restart Hub to run recovery before taking a backup. After a successful stop, back up `hub.sqlite3`, its `-wal` file if present, `hub.master.key`, and the entire `blobs/` directory together. Include the blob directory's `database.owner` file. Keep the matching durable admin credential in secure storage.

Restore the complete backup with Hub stopped, preserving private permissions and the original canonical database path. Hub checks that the master key matches the restored database before starting.
