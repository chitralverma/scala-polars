set shell := ["bash", "-c"]
set ignore-comments := true

root := justfile_directory()
native_root := "native"
native_manifest := "native/Cargo.toml"
cargo_flags := env("CARGO_FLAGS", "--locked")

# Default recipe to 'help' to display this help screen
[private]
default: help

# Display this help screen
help:
    @just --list

# Display this help screen
[private]
echo-command args:
    @echo "{{ style("command") }}--- {{ args }} ---{{ NORMAL }}"

# Format all code (Scala, Java, Rust, sbt)
[group('lint')]
fmt:
    @just echo-command 'Formatting Scala, Java & Sbt'
    @sbt -error --batch scalafmtAll scalafmtSbt javafmtAll
    @just echo-command 'Formatting Rust'
    @cargo clippy -q {{ cargo_flags }} --no-deps --fix --allow-dirty --allow-staged --manifest-path {{ native_manifest }}
    @cargo sort {{ native_root }}
    @cargo fmt --quiet --manifest-path {{ native_manifest }}
    @just --fmt --unstable

# Check formatting and linting
[group('lint')]
lint:
    @just echo-command 'Checking Scala, Java & Sbt'
    @sbt -error --batch scalafmtCheckAll scalafmtSbtCheck javafmtCheckAll
    @just echo-command 'Checking Rust'
    @cargo clippy -q {{ cargo_flags }} --no-deps --manifest-path {{ native_manifest }} -- -D warnings
    @cargo sort {{ native_root }} --check
    @cargo fmt --check --manifest-path {{ native_manifest }}
    @just --fmt --unstable --check

# Run all code formatting and quality checks
[group('lint')]
pre-commit: fmt lint

# Generate JNI headers
[group('dev')]
gen-headers: clean-headers
    @sbt -error --batch genHeaders

# Remove generated JNI headers
[group('dev')]
clean-headers:
    @rm -rf core/target/native
    @just echo-command 'Removed JNI headers directory'

# Build native library TARGET_TRIPLE, NATIVE_RELEASE, NATIVE_LIB_LOCATION env vars are supported
[group('dev')]
build-native:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "${SKIP_NATIVE_GENERATION:-false}" = "false" ]; then
        TRIPLE="${TARGET_TRIPLE:-$(rustc -vV | grep host | cut -d' ' -f2)}"
        ARCH=$(echo "$TRIPLE" | cut -d'-' -f1)
        RELEASE_FLAG=""
        if [ "${NATIVE_RELEASE:-false}" = "true" ]; then
            RELEASE_FLAG="--release"
        fi

        # Generate native library artifacts in a predictable output directory
        NATIVE_OUTPUT_DIR="core/target/native-libs/$ARCH"
        mkdir -p "$NATIVE_OUTPUT_DIR"
        cargo build {{ cargo_flags }} --manifest-path {{ native_manifest }} -Z unstable-options $RELEASE_FLAG --lib --target "$TRIPLE" --artifact-dir "$NATIVE_OUTPUT_DIR"

        if [ -n "${NATIVE_LIB_LOCATION:-}" ]; then
            # Remove trailing slash and normalize path
            CLEAN_PATH="${NATIVE_LIB_LOCATION%/}"
            DEST="$CLEAN_PATH/$ARCH"
            
            echo "Copying built native library from '$NATIVE_OUTPUT_DIR' to '$DEST'..."
            mkdir -p "$DEST"
            cp -rf "$NATIVE_OUTPUT_DIR"/* "$DEST/"
            
            # Verify the copy succeeded
            if [ -z "$(ls -A "$DEST")" ]; then
                echo "Error: Failed to copy native libraries to $DEST"
                exit 1
            fi
        fi
    else
        @just echo-command 'Environment variable SKIP_NATIVE_GENERATION is set, skipping cargo build.'
    fi

# Build assembly jars
[group('dev')]
assembly: build-native
    sbt +assembly

# Compile
[group('dev')]
compile: build-native
    sbt compile

# Clean build artifacts
[group('dev')]
clean: clean-headers
    @sbt clean cleanFiles
    @cargo clean --manifest-path {{ native_manifest }}

# Run tests
[group('dev')]
test:
    sbt +test

# Generate documentation
[group('release')]
site:
    @sbt makeSite

# Publish documentation
[group('release')]
publish-site:
    @sbt ghpagesPushSite

# Release artifacts
[group('release')]
release:
    @sbt ci-release
