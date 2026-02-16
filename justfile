set shell := ["bash", "-c"]
set ignore-comments := true

root := justfile_directory()
native_root := root / 'native'
native_manifest := root / 'native' / 'Cargo.toml'

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
    @cargo clippy -q --frozen --no-deps --fix --allow-dirty --allow-staged --manifest-path {{ native_manifest }}
    @cargo sort {{ native_root }}
    @cargo fmt --quiet --manifest-path {{ native_manifest }}
    @just --fmt --unstable

# Check formatting and linting
[group('lint')]
lint:
    @just echo-command 'Checking Scala, Java & Sbt'
    @sbt -error --batch scalafmtCheckAll scalafmtSbtCheck javafmtCheckAll
    @just echo-command 'Checking Rust'
    @cargo clippy -q --frozen --no-deps --manifest-path {{ native_manifest }} -- -D warnings
    @cargo sort {{ native_root }} --check
    @cargo fmt --check --manifest-path {{ native_manifest }}
    @just --fmt --unstable --check

# Run all code formatting and quality checks
[group('lint')]
pre-commit: fmt lint

# Build native library TARGET_TRIPLE, NATIVE_RELEASE, NATIVE_LIB_LOCATION env vars are supported
[group('dev')]
build-native:
    #!/bin/sh
    set -euo pipefail
    if [ '{{ env('SKIP_NATIVE_GENERATION', 'false') }}' == "false" ]; then
        TRIPLE={{ env('TARGET_TRIPLE', `rustc -vV | grep host | cut -d' ' -f2`) }}
        ARCH=$(echo $TRIPLE | cut -d'-' -f1)
        RELEASE_FLAG=""
        if [ '{{ env('NATIVE_RELEASE', 'false') }}' == "true" ]; then
            RELEASE_FLAG="--release"
        fi

        # Generate native library artifacts in a predictable output directory
        NATIVE_OUTPUT_DIR={{ root / 'core' / 'target' / 'native-libs' / '$ARCH' }}
        mkdir -p "$NATIVE_OUTPUT_DIR"
        cargo build --manifest-path {{ native_manifest }} -Z unstable-options $RELEASE_FLAG --lib --target $TRIPLE --artifact-dir $NATIVE_OUTPUT_DIR

        if [ -n '{{ env('NATIVE_LIB_LOCATION', '') }}' ]; then
            DEST={{ '$NATIVE_LIB_LOCATION' / '$ARCH' }}
            echo "Environment variable NATIVE_LIB_LOCATION is set, copying built native library from location '$NATIVE_OUTPUT_DIR' to '$DEST'."
            mkdir -p "$DEST"
            cp -r $NATIVE_OUTPUT_DIR/* "$DEST/"
        fi
    else
        just echo-command 'Environment variable SKIP_NATIVE_GENERATION is set, skipping cargo build.'
    fi

# Build assembly jars
[group('dev')]
assembly: build-native
    sbt +assembly

# Complie
[group('dev')]
compile: build-native
    sbt compile

# Clean build artifacts
[group('dev')]
clean:
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
