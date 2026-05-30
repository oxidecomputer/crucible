# Crucible fork — upstream tracking

Seed's fork of Crucible. Seed runs the `downstairs`, `nbd_server` (Upstairs),
and `crucible-pantry` binaries built from this fork as seed-agent-supervised
subprocesses; the `seed` crate links **none** of Crucible's code.

## Upstream

- Repo: https://github.com/oxidecomputer/crucible
- Base commit: `e7af674f27ed04ce27739edee96829f7d7d5e6c0` — "update Omicron (#1940)"

Add the upstream remote to sync:

    git remote add upstream https://github.com/oxidecomputer/crucible
    git fetch upstream

## Seed patch — iroh transport swap

An additional `seed/crucible/v1` transport alongside Crucible's TCP path, so
Upstairs↔Downstairs traffic rides Seed's iroh mesh (NodeCert + ALPN ACL) instead
of plain TCP. Patched files (relative to the base):

- `common/src/seed_iroh.rs` (new) — endpoint build/connect/accept; env-driven secret + targets
- `common/src/lib.rs` — `pub mod seed_iroh;`
- `upstairs/src/client.rs` — iroh dial branch before the TCP connect
- `downstairs/src/lib.rs` — `WrappedStream::Iroh` + iroh-accept loop
- `downstairs/src/main.rs`, `nbd_server/src/main.rs`, `crutest/src/main.rs` — `--seed-iroh-*` args
- root `Cargo.toml`, `common/Cargo.toml`, `downstairs/Cargo.toml` — iroh deps; `rust-toolchain.toml` removed
- `downstairs/src/admin.rs` — `iroh_endpoint: None` field plumb

The patch must live as commits on a **`seed/iroh-transport`** branch on top of the
base — NOT as uncommitted working-tree edits. `publish-crucible.sh` builds from a
clean checkout at a committed SHA and refuses a dirty tree (provenance integrity).

## Syncing a new upstream release

1. `git fetch upstream`
2. `git checkout seed/iroh-transport && git rebase <new-upstream-sha>` (or merge)
3. Resolve conflicts in the patched files above; re-run the iroh smoke test
4. Update the base commit recorded here
5. Build + publish via `seed/scripts/builder/publish-crucible.sh <fork-sha>`

## Binary provenance

`publish-crucible.sh` stamps every published artifact with the fork SHA (which
encodes base + patch) and the upstream base SHA in a `*.provenance.json` sidecar,
and addresses artifacts by fork SHA in R2 (`binaries/crucible/<sha>/`). A deployed
binary therefore maps to exactly one (upstream base + patch) version.

## Maintenance owner

Crucible-upstream tracking owner: **TODO — assign** (see seed project tracking).
