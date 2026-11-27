# API contracts

`openapi-hub.json` is generated from the Hub's Rust routes and schema types. The `gate_openapi_drift` test compares this file with the output of `cooplet-hub openapi`. The same bytes are served at `GET /v1/openapi.json`.

Run this command from the workspace root to regenerate the contract.

```sh
make exec toolchain=rust command='cargo run --quiet --locked --offline -p cooplet-hub -- openapi' > contracts/openapi-hub.json
```
