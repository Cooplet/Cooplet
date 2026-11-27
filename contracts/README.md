# API contracts

`openapi-node.json` and `openapi-hub.json` are generated from the Node and Hub Rust routes and schema types. Each crate's `gate_openapi_drift` test compares its checked-in file against the generated schema, and each service serves the same bytes at `GET /v1/openapi.json`.

Run these commands from the workspace root to regenerate the contracts.

```sh
make exec toolchain=rust command='cargo run --quiet --locked --offline -p cooplet-node -- openapi' > contracts/openapi-node.json
make exec toolchain=rust command='cargo run --quiet --locked --offline -p cooplet-hub -- openapi' > contracts/openapi-hub.json
```
