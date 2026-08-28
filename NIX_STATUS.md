# Nix Flake Status for Crucible

## Current State

The Nix flake is **fully functional** and provides reproducible builds for all Crucible binaries.

### ✅ Working Components

1. **Binary Builds** - All packages build successfully:
   - `crucible-downstairs` - Storage server
   - `crucible-agent` - Production agent
   - `crucible-pantry` - Pantry service
   - `crutest` - Test client
   - `dsc` - Downstairs controller
   - `crucible-hammer` - Stress testing tool

2. **Development Shell** - `nix develop` provides:
   - Exact Rust 1.90.0 toolchain
   - All system dependencies (sqlite, openssl, pkg-config)
   - Development tools (cargo-watch, rust-analyzer)

3. **Helper Scripts** - Functional app wrappers:
   - `start-downstairs` - Start N downstairs instances via DSC
   - `test-up` - Wrapper for tools/test_up.sh
   - `test-dsc` - Wrapper for tools/test_dsc.sh
   - `test-cluster` - Full test workflow

4. **Docker Images**:
   - `downstairs-docker` - Single downstairs container
   - `test-cluster-docker` - Test cluster with 3 downstairs

## Build Approach

The flake uses `stdenv.mkDerivation` with cargo to build the entire workspace at once. This approach:

1. **Handles git dependencies correctly** - Cargo fetches Omicron and other git dependencies naturally
2. **Builds all binaries in one pass** - More efficient than per-package builds
3. **Requires relaxed sandbox** - Uses `__noChroot = true` to allow network access

## Usage

### Building Packages

```bash
# Build with relaxed sandbox (required for git dependencies)
nix build .#dsc --option sandbox relaxed
nix build .#crucible-downstairs --option sandbox relaxed
nix build .#crucible-test-tools --option sandbox relaxed
```

### Running Applications

```bash
# Start 5 downstairs instances
nix run .#start-downstairs --option sandbox relaxed -- -n 5

# Run test cluster
nix run .#test-cluster --option sandbox relaxed

# Run full test suite
nix run .#test-up --option sandbox relaxed
```

### Docker Images

```bash
# Build and load downstairs image
nix build .#downstairs-docker --option sandbox relaxed
docker load < result
docker run -v /data:/data -p 8810:8810 crucible-downstairs:latest run -d /data -p 8810
```

## Configuration

To avoid typing `--option sandbox relaxed` every time, add to `/etc/nix/nix.conf` or `~/.config/nix/nix.conf`:

```
sandbox = relaxed
```

Then you can simply use:
```bash
nix build .#dsc
nix run .#start-downstairs -- -n 5
```

## Why Relaxed Sandbox?

Crucible has complex git dependencies from the Omicron project. Standard Nix approaches (buildRustPackage, crane) struggle with:

1. **Circular dependencies** in Omicron git repository
2. **Missing files** referenced in Cargo.toml (README.md paths)
3. **Complex workspace structures** spanning multiple repositories

The solution: let cargo handle dependency resolution by allowing network access during build. This is safe because:
- Source code is still from the local directory (reproducible)
- Cargo.lock pins exact dependency versions (deterministic)
- Only git fetching needs network access

## Advantages Over Cargo

Even with relaxed sandbox, the Nix flake provides benefits over plain cargo:

1. **Exact Rust version** - Always 1.90.0, never drifts
2. **System dependencies** - sqlite, openssl automatically available
3. **Reproducible across machines** - Same inputs = same outputs
4. **Helper scripts** - Ready-to-use start-downstairs, test-cluster commands
5. **Docker images** - One command to build containers
6. **No local rust installation needed** - Everything from Nix

## Technical Details

The build process:

1. Nix provides Rust 1.90.0 toolchain and system libraries
2. `stdenv.mkDerivation` copies source to build directory
3. `cargo build --release --workspace --bins` runs with network access
4. Binaries are installed to Nix store
5. Individual packages extract specific binaries from the workspace build

This is more efficient than vendoring dependencies upfront because:
- Cargo's dependency resolution is battle-tested
- Git dependencies are fetched on-demand
- Workspace builds share compilation artifacts

## Comparison with Other Approaches

| Approach | Status | Notes |
|----------|--------|-------|
| **buildRustPackage** | ❌ Failed | Vendoring breaks with complex git deps |
| **crane** | ❌ Failed | Same vendoring issues, missing README files |
| **stdenv + cargo** | ✅ Works | Requires relaxed sandbox, fully functional |
| **Pure cargo** | ✅ Works | No Nix benefits, version drift risk |

## Future Improvements

1. **Stricter builds** - Investigate if outputHashes can be pre-computed for all git deps
2. **Caching** - Set up binary cache to avoid rebuilds
3. **Cross-compilation** - Add aarch64 support
4. **NixOS module** - Systemd service definitions
5. **Hydra CI** - Continuous integration with Nix

## Conclusion

The Crucible Nix flake is production-ready and provides significant value:

- ✅ Reproducible builds with exact Rust version
- ✅ All binaries build successfully
- ✅ Helper scripts and Docker images
- ✅ Development shell with correct dependencies
- ⚠️ Requires `sandbox = relaxed` configuration

This is a pragmatic solution that works today while maintaining most of Nix's reproducibility benefits.
