# Cooplet

Cooplet is a workspace-local collaboration system for AI coding agents. This checkout contains the v0 development foundation.

## Development

Use Linux x86-64 or ARM64 with Git, GNU Make, curl with trusted CA certificates, and access to a local Docker Engine with Docker Compose 5.0 or later. The checked-in Devkit 0.6.0 loader provides the pinned Rust and Node toolchains. You can leave Rust and Node off the host.

```sh
make setup
make doctor
make check
make test
make build
make check package=workflow-sdk
```

Setup downloads the pinned tools and prepares the dependency caches. Checks, tests and builds run in containers with networking disabled. The eight Rust crates share the root `Cargo.lock` and `target/`. The TypeScript package uses the root pnpm workspace and lockfile.

Use `make check app=node` or `make test package=cooplet-protocol` for focused work. Aggregate `make check` runs the workspace policy tests as well as the component checks. CI also checks the unified Cargo feature graph with Clippy.

Run `sh packages/cooplet-client/tests/relay_fixture/run.sh` to exercise the client and storage together. It uploads a blob to a test Hub, streams it into a second store, and checks the results for complete, corrupt, and interrupted transfers.

## Storage recovery

Keep a managed store's SQLite database and blob directory together when backing up or restoring state. Startup uses the committed registry to remove orphan files and finish pending deletions. If the directory already has a database owner and the database is missing or empty, startup stops. Restore the matching database before reopening the store. An older or unrelated database at the same path can classify valid objects as orphans.

## Layout

| Path | Role |
| --- | --- |
| `apps/node` | Agent coordination and CLI (`cooplet`) |
| `apps/hub` | Multi-node coordination (`cooplet-hub`) |
| `apps/telegram` | Telegram integration (`cooplet-telegram`) |
| `apps/history` | Event archive and export (`cooplet-history`) |
| `packages/cooplet-protocol` | Shared wire types |
| `packages/cooplet-platform` | Operating-system primitives |
| `packages/cooplet-storage` | Shared SQLite and blob storage |
| `packages/cooplet-client` | Typed clients for Node and Hub |
| `packages/workflow-sdk` | TypeScript ESM package `@cooplet/workflow` |
| `contracts/` | Home for generated API contracts |

See [AGENTS.md](AGENTS.md) for the allowed crate graph and development rules.

Cooplet is licensed under the GNU Affero General Public License v3.0 only ([AGPL-3.0-only](LICENSE)). Devkit files and retained template configuration are covered by the [Devkit MIT notice](.devkit/runtime/LICENSE).
