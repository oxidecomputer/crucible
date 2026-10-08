# Nix Usage Guide for Crucible

This guide shows how to use the Nix flake for Crucible development and testing.

## Prerequisites

- Nix with flakes enabled
- Git (to clone the repository)

Enable flakes in `/etc/nix/nix.conf` or `~/.config/nix/nix.conf`:
```
experimental-features = nix-command flakes
```

## Development Environment

### Enter the Development Shell

The development shell provides Rust 1.90.0 (matching rust-toolchain.toml) and all dependencies:

```bash
nix develop
```

Inside the shell, you can use cargo normally:
```bash
cargo build --release
cargo test -p crucible-downstairs
cargo run -p dsc -- --help
```

### Development Tools Available

The shell includes:
- Rust 1.90.0 with rust-src and rust-analyzer
- System libraries: sqlite, openssl, pkg-config
- Development tools: cargo-watch

### Why Use the Nix Shell?

1. **Reproducibility** - Everyone gets exact Rust 1.90.0
2. **No version drift** - System Rust updates won't break builds
3. **Clean environment** - Isolated from system packages
4. **Cross-platform** - Works on Linux and macOS

## Building with Cargo (Recommended)

Currently, building Rust binaries directly with Nix has issues due to complex Omicron git dependencies. The recommended approach is:

```bash
# Enter Nix shell for correct Rust version
nix develop

# Build with cargo
cargo build --release

# Binaries will be in target/release/
ls target/release/{crucible-downstairs,dsc,crutest,crucible-hammer}
```

## Running Tests

### Using Existing Test Scripts

The flake provides wrappers for existing test scripts. After building with cargo:

```bash
# Set BINDIR to your cargo build output
export BINDIR=$PWD/target/release

# Run unencrypted tests
bash tools/test_up.sh unencrypted

# Run encrypted tests
bash tools/test_up.sh encrypted

# Run DSC tests
bash tools/test_dsc.sh
```

### Using DSC Directly

Start 3 downstairs instances:
```bash
# Create regions
$BINDIR/dsc create --region-count 3 --extent-count 10 --extent-size 10 \
  --ds-bin $BINDIR/crucible-downstairs \
  --region-dir /var/tmp/crucible-test \
  --output-dir /var/tmp/crucible-test/dsc-output

# Start downstairs
$BINDIR/dsc start --region-count 3 \
  --ds-bin $BINDIR/crucible-downstairs \
  --region-dir /var/tmp/crucible-test \
  --output-dir /var/tmp/crucible-test/dsc-output &

DSC_PID=$!
echo "DSC started at PID: $DSC_PID"
```

Check downstairs status:
```bash
$BINDIR/dsc cmd state -c 0  # Check client 0
$BINDIR/dsc cmd state -c 1  # Check client 1
$BINDIR/dsc cmd state -c 2  # Check client 2
```

Run tests:
```bash
$BINDIR/crutest one -g 1 --verify-at-end --dsc "127.0.0.1:9998"
```

Shutdown:
```bash
$BINDIR/dsc cmd shutdown
```

## Flake Structure

The flake provides these outputs:

### Packages (currently have build issues)

- `crucible-downstairs` - Storage server binary
- `crucible-agent` - Agent for production deployment
- `crucible-pantry` - Pantry service
- `crutest` - Test client
- `dsc` - Downstairs controller
- `crucible-hammer` - Stress testing tool
- `crucible-test-tools` - Combined package with core tools
- `downstairs-docker` - Docker image for single downstairs
- `test-cluster-docker` - Docker image for test cluster

### Apps (use cargo-built binaries)

These require BINDIR to be set or binaries in PATH:

- `start-downstairs` - Start N downstairs instances via DSC
- `test-up` - Run full test suite (wrapper for tools/test_up.sh)
- `test-dsc` - Run DSC tests (wrapper for tools/test_dsc.sh)
- `test-cluster` - Start cluster and run tests

### Development Shell

- `devShells.default` - Development environment with Rust 1.90.0

## Future Work

To make binary builds work with Nix:

1. **Migrate to crane** - Better handling of complex cargo workspaces
2. **Patch dependencies** - Fix the mg-admin-client types module issue
3. **Upstream fixes** - Work with Omicron team on dependency structure
4. **Release tarballs** - Use released artifacts without git dependencies

See [NIX_STATUS.md](NIX_STATUS.md) for technical details on the build issues.

## Quick Reference

```bash
# Development workflow
nix develop                    # Enter shell
cargo build --release          # Build binaries
cargo test                     # Run tests

# Check flake structure
nix flake show                 # Show all outputs
nix flake check                # Validate flake

# Try building (currently fails)
nix build .#dsc                # Build dsc binary
nix build .#crucible-downstairs

# Update flake inputs
nix flake update               # Update nixpkgs and rust-overlay
nix flake lock                 # Regenerate flake.lock
```

## Getting Help

- Flake source: `flake.nix`
- Build status: `NIX_STATUS.md`
- Main README: `README.md`
- Crucible RFDs: https://rfd.shared.oxide.computer/rfd/0060

## Example: Complete Test Workflow

```bash
# 1. Enter Nix shell
nix develop

# 2. Build binaries
cargo build --release
export BINDIR=$PWD/target/release

# 3. Create and start downstairs
$BINDIR/dsc create --region-count 3 --extent-count 10 --extent-size 10 \
  --ds-bin $BINDIR/crucible-downstairs \
  --region-dir /tmp/crucible-test \
  --output-dir /tmp/crucible-test/dsc &

DSC_PID=$!

# 4. Wait for startup
sleep 5

# 5. Run tests
$BINDIR/crutest one -g 1 --verify-at-end --dsc "127.0.0.1:9998"

# 6. Cleanup
$BINDIR/dsc cmd shutdown
wait $DSC_PID
rm -rf /tmp/crucible-test
```

## Notes

- The Crucible protocol requires exactly **3 downstairs** for replication and quorum
- Port range 8810-8830 (default) must be available
- Each downstairs needs writable storage directory
- DSC default control port is 9998
