#!/bin/sh
# Run from a checkout with the pinned Devkit dependencies prepared.
# The isolated consumer uses real path crates and its own offline lock.
set -eu
umask 022
cd "$(dirname "$0")/../../../.."

fixture=packages/cooplet-client/tests/relay_fixture
consumer=.artifacts/2-core-foundation-crates/relay-consumer
mkdir -p "$consumer/src"
cp Cargo.lock "$consumer/Cargo.lock"
cp "$fixture/Cargo.toml.in" "$consumer/Cargo.toml"
cp "$fixture/relay.rs" "$consumer/src/main.rs"

make exec toolchain=rust write=1 network=none \
    command="cargo check --manifest-path $consumer/Cargo.toml --offline"
make setup
make exec toolchain=rust network=none \
    command="cargo run --manifest-path $consumer/Cargo.toml --locked --offline"
