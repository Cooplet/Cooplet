# Cooplet Hub

Run `make build app=hub` from the repository root to build `target/debug/cooplet-hub`. Run the examples below from the same directory.

## Initialize and run

```sh
./target/debug/cooplet-hub init --state-dir ./hub-state --display-name "Local Hub"
./target/debug/cooplet-hub run --state-dir ./hub-state --listen 127.0.0.1:8200 \
  --public-origin http://127.0.0.1:8200
```

If you create the state directory yourself, give it mode `0700`. Record the `hub_id` printed by `init`. The initial administrator bearer is in `hub-state/admin.token` (mode `0600`). Store a secure copy of a current durable admin credential. Running `init` against the same initialized Hub returns its existing ID and leaves the credential files alone.

Hub serves plain HTTP on its listening socket. For remote access, set `--public-origin` to the HTTPS origin of a TLS-terminating reverse proxy. Restrict access to the upstream socket to the proxy and apply client IP rate limits at the proxy. Allow streaming responses without proxy buffering for the NDJSON event stream. Plain HTTP origins are for local use with both the bind address and public origin on loopback IP addresses.

Clients derive their Hub pin from a trusted `hub_id`. `GET /v1/info` exposes the server identity and API version, and `GET /v1/openapi.json` exposes the API contract. The CLI command `cooplet-hub openapi` prints the same contract without starting a service.

## Enroll a node

```sh
./target/debug/cooplet-hub enrollment-token create --hub http://127.0.0.1:8200 \
  --hub-id "$HUB_ID" --token-file ./hub-state/admin.token --name "Build node"
```

Set `HUB_ID` to the ID recorded during initialization. For remote enrollment, use the Hub's HTTPS origin. If it uses a private CA, pass its PEM certificate with `--ca-cert`. The default enrollment lifetime is 15 minutes and `--ttl` accepts up to 24 hours. The JSON printed to stdout contains the enrollment token once, plus its `token_id` and reserved `node_id`. Treat the JSON as a credential and deliver the token securely to the enrolling node.

The CLI prints the idempotency key to stderr before making the request. If the issuance response is lost, repeat the same command with `--idempotency-key` set to the original key. A committed issuance returns its IDs and `already_issued: true` for 30 days. The secret is available only in the original successful response. Revoke the old credential before issuing a replacement with a new key.

If you lose the redemption response, use the recorded `token_id` to call `POST /v1/enrollment-tokens/{token_id}/revoke`. Revoke the reserved node with `POST /v1/nodes/{node_id}/revoke` if the first call reports `already_consumed: true` or the enrollment record is gone (`404`). For a definitive check, call both endpoints in that order. Use an admin bearer, the Hub pin and a distinct idempotency key for each call. A `404` from the node revocation means the reserved node ID has no current registration. Generate a new enrollment token for the next attempt.

## Stop and back up

Send `SIGINT` or `SIGTERM` and wait for Hub to exit successfully. Once stopped, back up `hub.sqlite3`, its `-wal` file if present, `hub.master.key`, and the entire `blobs/` directory together. Include the blob directory's `database.owner` file. Keep the matching durable admin credential in secure storage.

Restore the complete backup with Hub stopped, preserving private permissions and the original canonical database path. Hub checks that the master key matches the restored database before starting.
