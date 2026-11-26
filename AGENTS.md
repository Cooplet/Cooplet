# Contributing to Cooplet

Start with the prerequisites in [README.md](README.md). Run `make setup` after cloning or changing dependencies, then use `make doctor`, `make check`, `make test`, and `make build`. Use `app=<name>` or `package=<name>` for targeted runs. Use `make check package=workflow-sdk` for the SDK type check.

Keep Rust dependencies in the root Cargo workspace and let crates inherit the shared pins. Change dependencies with the pinned toolchain through `make exec toolchain=rust write=1 network=bridge command='cargo …'`, then run `make sync` and `make setup`. Use Devkit lifecycle commands to update the managed `.devkit/` files.

The workspace fitness tests enforce these internal dependency permissions.

| Crate | Allowed internal dependencies |
| --- | --- |
| `cooplet-protocol` | None |
| `cooplet-platform` | None |
| `cooplet-storage` | `cooplet-protocol`, `cooplet-platform` |
| `cooplet-client` | `cooplet-protocol` |
| `apps/node` | `cooplet-protocol`, `cooplet-platform`, `cooplet-storage`, `cooplet-client` |
| `apps/hub` | `cooplet-protocol`, `cooplet-platform`, `cooplet-storage`, `cooplet-client` (dev only) |
| `apps/telegram`, `apps/history` | `cooplet-protocol`, `cooplet-platform`, `cooplet-client` |

Keep the protocol free of I/O. It may not depend on Tokio, SQLite, Axum, reqwest, or libc. The client may not depend on Axum or SQLite. Keep unsafe code confined to `cooplet-platform` with a safety argument for each block. Keep production Rust free of unchecked unwraps, panics, and other constructs denied by the workspace Clippy policy.

Preserve the gcc-only build and ring-backed Rustls configuration when adding dependencies. Keep workflow SDK code strict, ESM, and erasable by a TypeScript-stripping runtime. Generate API contracts from Rust code when adding endpoints.
