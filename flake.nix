{
  description = "Crucible - distributed network-replicated block storage system";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        # Exact Rust 1.90.0 toolchain from rust-toolchain.toml
        rustToolchain = pkgs.rust-bin.stable."1.90.0".default.override {
          extensions = [ "rust-src" "rust-analyzer" ];
        };

        # Common build inputs for Rust packages
        buildInputs = with pkgs; [
          sqlite
          openssl
        ] ++ lib.optionals stdenv.hostPlatform.isDarwin [
          darwin.apple_sdk.frameworks.Security
          darwin.apple_sdk.frameworks.SystemConfiguration
        ];

        nativeBuildInputs = with pkgs; [
          pkg-config
          rustToolchain
        ];

        # Build using cargo directly (works around Nix vendoring issues with complex git deps)
        crucible-workspace = pkgs.stdenv.mkDerivation {
          pname = "crucible-workspace";
          version = "0.1.0";

          src = ./.;

          inherit buildInputs nativeBuildInputs;

          buildPhase = ''
            export CARGO_HOME=$TMPDIR/cargo
            export HOME=$TMPDIR
            ${rustToolchain}/bin/cargo build --release --workspace --bins
          '';

          installPhase = ''
            mkdir -p $out/bin
            cp target/release/crucible-downstairs $out/bin/
            cp target/release/crucible-agent $out/bin/
            cp target/release/crucible-pantry $out/bin/
            cp target/release/crutest $out/bin/
            cp target/release/dsc $out/bin/
            cp target/release/crucible-hammer $out/bin/ || true
          '';

          # Allow network access for fetching git dependencies
          __noChroot = true;
        };

        # Extract individual binaries
        crucible-downstairs = pkgs.runCommand "crucible-downstairs" {} ''
          mkdir -p $out/bin
          cp ${crucible-workspace}/bin/crucible-downstairs $out/bin/
        '';

        crucible-agent = pkgs.runCommand "crucible-agent" {} ''
          mkdir -p $out/bin
          cp ${crucible-workspace}/bin/crucible-agent $out/bin/
        '';

        crucible-pantry = pkgs.runCommand "crucible-pantry" {} ''
          mkdir -p $out/bin
          cp ${crucible-workspace}/bin/crucible-pantry $out/bin/
        '';

        crutest = pkgs.runCommand "crutest" {} ''
          mkdir -p $out/bin
          cp ${crucible-workspace}/bin/crutest $out/bin/
        '';

        dsc = pkgs.runCommand "dsc" {} ''
          mkdir -p $out/bin
          cp ${crucible-workspace}/bin/dsc $out/bin/
        '';

        crucible-hammer = pkgs.runCommand "crucible-hammer" {} ''
          mkdir -p $out/bin
          cp ${crucible-workspace}/bin/crucible-hammer $out/bin/ || true
        '';

        # Combined package
        crucible-test-tools = pkgs.symlinkJoin {
          name = "crucible-test-tools";
          paths = [
            crucible-downstairs
            crutest
            dsc
          ];
        };

        # Helper script to start N downstairs instances
        start-downstairs-script = pkgs.writeShellScriptBin "start-downstairs" ''
          set -euo pipefail

          # Default values
          COUNT=3
          PORT_BASE=8810
          DATA_DIR="/var/tmp/crucible-nix"
          ENCRYPTED=""
          EXTENT_COUNT=10
          EXTENT_SIZE=10

          # Parse arguments
          while getopts "n:p:d:e" opt; do
            case $opt in
              n) COUNT=$OPTARG ;;
              p) PORT_BASE=$OPTARG ;;
              d) DATA_DIR=$OPTARG ;;
              e) ENCRYPTED="--encrypted" ;;
              *)
                echo "Usage: $0 [-n COUNT] [-p PORT_BASE] [-d DATA_DIR] [-e]"
                echo "  -n COUNT      Number of downstairs (default: 3)"
                echo "  -p PORT_BASE  Base port (default: 8810, increments by 10)"
                echo "  -d DATA_DIR   Data directory (default: /var/tmp/crucible-nix)"
                echo "  -e            Enable encryption"
                exit 1
                ;;
            esac
          done

          echo "Starting $COUNT downstairs instances..."
          echo "Port base: $PORT_BASE"
          echo "Data directory: $DATA_DIR"

          # Create regions
          echo "Creating regions..."
          ${dsc}/bin/dsc create \
            --region-count "$COUNT" \
            --extent-count "$EXTENT_COUNT" \
            --extent-size "$EXTENT_SIZE" \
            --ds-bin ${crucible-downstairs}/bin/crucible-downstairs \
            --region-dir "$DATA_DIR" \
            --output-dir "$DATA_DIR/dsc-output" \
            --port-base "$PORT_BASE" \
            $ENCRYPTED

          # Start downstairs
          echo "Starting downstairs..."
          ${dsc}/bin/dsc start \
            --region-count "$COUNT" \
            --ds-bin ${crucible-downstairs}/bin/crucible-downstairs \
            --region-dir "$DATA_DIR" \
            --output-dir "$DATA_DIR/dsc-output" \
            --port-base "$PORT_BASE" &

          DSC_PID=$!
          echo "DSC started with PID: $DSC_PID"
          echo "Waiting for downstairs to start..."
          sleep 5

          # Check if dsc is still running
          if ! kill -0 $DSC_PID 2>/dev/null; then
            echo "Error: DSC failed to start"
            exit 1
          fi

          echo ""
          echo "Downstairs instances started successfully!"
          echo "To check status: ${dsc}/bin/dsc cmd state -c 0"
          echo "To shutdown: ${dsc}/bin/dsc cmd shutdown"
          echo ""
          echo "DSC is running in the background (PID: $DSC_PID)"
          echo "Use 'kill $DSC_PID' to stop all downstairs instances"

          # Keep the script running
          wait $DSC_PID
        '';

        # Wrapper for test_up.sh
        test-up-script = pkgs.writeShellScriptBin "test-up" ''
          set -euo pipefail

          BINDIR=${crucible-test-tools}/bin
          export BINDIR

          ${pkgs.bash}/bin/bash ${./tools/test_up.sh} "$@"
        '';

        # Wrapper for test_dsc.sh
        test-dsc-script = pkgs.writeShellScriptBin "test-dsc" ''
          set -euo pipefail

          BINDIR=${crucible-test-tools}/bin
          export BINDIR

          ${pkgs.bash}/bin/bash ${./tools/test_dsc.sh}
        '';

        # Complete test workflow: start 3 downstairs + run tests
        test-cluster-script = pkgs.writeShellScriptBin "test-cluster" ''
          set -euo pipefail

          DATA_DIR="/var/tmp/crucible-test-cluster"
          DSC_OUTPUT="$DATA_DIR/dsc-output"

          # Cleanup function
          cleanup() {
            echo "Cleaning up..."
            ${dsc}/bin/dsc cmd shutdown || true
            rm -rf "$DATA_DIR"
          }

          trap cleanup EXIT INT TERM

          echo "Creating test cluster with 3 downstairs..."
          mkdir -p "$DSC_OUTPUT"

          # Create 3 regions
          ${dsc}/bin/dsc create \
            --region-count 3 \
            --extent-count 10 \
            --extent-size 10 \
            --ds-bin ${crucible-downstairs}/bin/crucible-downstairs \
            --region-dir "$DATA_DIR" \
            --output-dir "$DSC_OUTPUT"

          # Start downstairs in background
          ${dsc}/bin/dsc start \
            --region-count 3 \
            --ds-bin ${crucible-downstairs}/bin/crucible-downstairs \
            --region-dir "$DATA_DIR" \
            --output-dir "$DSC_OUTPUT" &

          DSC_PID=$!
          echo "DSC started with PID: $DSC_PID"

          # Wait for startup
          sleep 5

          # Verify dsc is running
          if ! kill -0 $DSC_PID 2>/dev/null; then
            echo "Error: DSC failed to start"
            exit 1
          fi

          # Wait for all downstairs to be running
          for cid in 0 1 2; do
            while true; do
              state=$(${dsc}/bin/dsc cmd state -c "$cid" || echo "Unknown")
              if [ "$state" = "Running" ]; then
                echo "Downstairs $cid is running"
                break
              fi
              echo "Waiting for downstairs $cid (state: $state)..."
              sleep 2
            done
          done

          echo ""
          echo "All downstairs are running. Starting tests..."
          echo ""

          # Run crutest with provided arguments (default to 'one' test)
          TEST_ARGS=("$@")
          if [ ''${#TEST_ARGS[@]} -eq 0 ]; then
            TEST_ARGS=("one" "-g" "1" "--verify-at-end")
          fi

          ${crutest}/bin/crutest "''${TEST_ARGS[@]}" --dsc "127.0.0.1:9998"
          TEST_RESULT=$?

          if [ $TEST_RESULT -eq 0 ]; then
            echo ""
            echo "Tests passed!"
          else
            echo ""
            echo "Tests failed with exit code: $TEST_RESULT"
          fi

          exit $TEST_RESULT
        '';

      in
      {
        packages = {
          inherit crucible-downstairs crucible-agent crucible-pantry crutest dsc crucible-hammer;
          inherit crucible-test-tools crucible-workspace;

          default = crucible-test-tools;

          # Docker image for single downstairs
          downstairs-docker = pkgs.dockerTools.buildImage {
            name = "crucible-downstairs";
            tag = "latest";

            copyToRoot = pkgs.buildEnv {
              name = "image-root";
              paths = [
                crucible-downstairs
                pkgs.bash
                pkgs.coreutils
              ];
              pathsToLink = [ "/bin" ];
            };

            config = {
              Cmd = [ "${crucible-downstairs}/bin/crucible-downstairs" ];
              WorkingDir = "/data";
              Volumes = {
                "/data" = {};
              };
              ExposedPorts = {
                "8810/tcp" = {};
              };
            };
          };

          # Docker image for test cluster
          test-cluster-docker = pkgs.dockerTools.buildImage {
            name = "crucible-test-cluster";
            tag = "latest";

            copyToRoot = pkgs.buildEnv {
              name = "image-root";
              paths = [
                crucible-test-tools
                pkgs.bash
                pkgs.coreutils
                pkgs.findutils
              ];
              pathsToLink = [ "/bin" ];
            };

            config = {
              Cmd = [
                "${pkgs.bash}/bin/bash"
                "-c"
                ''
                  mkdir -p /data/dsc-output && \
                  ${dsc}/bin/dsc create --region-count 3 --extent-count 10 --extent-size 10 \
                    --ds-bin ${crucible-downstairs}/bin/crucible-downstairs \
                    --region-dir /data --output-dir /data/dsc-output && \
                  exec ${dsc}/bin/dsc start --region-count 3 \
                    --ds-bin ${crucible-downstairs}/bin/crucible-downstairs \
                    --region-dir /data --output-dir /data/dsc-output
                ''
              ];
              WorkingDir = "/data";
              Volumes = {
                "/data" = {};
              };
              ExposedPorts = {
                "8810/tcp" = {};
                "8820/tcp" = {};
                "8830/tcp" = {};
                "9998/tcp" = {}; # dsc control port
              };
            };
          };
        };

        apps = {
          start-downstairs = {
            type = "app";
            program = "${start-downstairs-script}/bin/start-downstairs";
          };

          test-up = {
            type = "app";
            program = "${test-up-script}/bin/test-up";
          };

          test-dsc = {
            type = "app";
            program = "${test-dsc-script}/bin/test-dsc";
          };

          test-cluster = {
            type = "app";
            program = "${test-cluster-script}/bin/test-cluster";
          };

          default = {
            type = "app";
            program = "${test-cluster-script}/bin/test-cluster";
          };
        };

        devShells.default = pkgs.mkShell {
          buildInputs = buildInputs ++ [
            rustToolchain
            pkgs.cargo-watch
          ] ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [
            # Linux-specific tools
          ];

          nativeBuildInputs = nativeBuildInputs;

          shellHook = ''
            echo "Crucible development environment"
            echo "Rust version: $(rustc --version)"
            echo ""
            echo "Available commands:"
            echo "  nix run .#start-downstairs          - Start 3 downstairs instances"
            echo "  nix run .#start-downstairs -- -n 5  - Start 5 downstairs instances"
            echo "  nix run .#test-up                   - Run unencrypted tests"
            echo "  nix run .#test-up -- encrypted      - Run encrypted tests"
            echo "  nix run .#test-dsc                  - Run dsc tests"
            echo "  nix run .#test-cluster              - Start cluster and run tests"
            echo ""
            echo "Build packages:"
            echo "  nix build .#crucible-downstairs     - Build downstairs binary"
            echo "  nix build .#dsc                     - Build dsc binary"
            echo "  nix build .#crutest                 - Build test client"
            echo ""
            echo "Docker images:"
            echo "  nix build .#downstairs-docker       - Build single downstairs image"
            echo "  nix build .#test-cluster-docker     - Build test cluster image"
            echo ""
          '';
        };
      }
    );
}
