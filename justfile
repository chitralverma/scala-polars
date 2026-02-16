set shell := ["bash", "-c"]

# Default goal
default:
    @just --list

# Format all code (Scala, Java, Rust, sbt)
fmt:
    sbt scalafmtAll scalafmtSbt javafmtAll
    cd native && cargo fix --allow-dirty --allow-staged && cargo sort && cargo fmt --all

# Check formatting and linting
lint:
    sbt scalafmtCheckAll scalafmtSbtCheck javafmtCheckAll
    cd native && cargo fmt --check --all && cargo sort --check && cargo clippy -- -D warnings

# Generate JNI headers
gen-headers: clean-headers
    sbt javah

# Remove generated JNI headers
clean-headers:
    rm -rf core/target/native

# Build native library
# TARGET_TRIPLE, NATIVE_RELEASE, NATIVE_LIB_LOCATION env vars are supported
build-native:
    #!/usr/bin/env bash
    set -e
    TRIPLE=${TARGET_TRIPLE:-$(rustc -vV | grep host | cut -d' ' -f2)}
    ARCH=$(echo $TRIPLE | cut -d'-' -f1)
    RELEASE_FLAG=""
    if [ "$NATIVE_RELEASE" == "true" ]; then
        RELEASE_FLAG="--release"
    fi
    # Use a predictable output directory for the native library
    NATIVE_OUTPUT_DIR="core/target/native-libs/$ARCH"
    mkdir -p "$NATIVE_OUTPUT_DIR"
    cd native && cargo build -Z unstable-options $RELEASE_FLAG --lib --target $TRIPLE --artifact-dir ../$NATIVE_OUTPUT_DIR
    
    if [ -n "$NATIVE_LIB_LOCATION" ]; then
        DEST="$NATIVE_LIB_LOCATION/$ARCH"
        mkdir -p "$DEST"
        cp -r $NATIVE_OUTPUT_DIR/* "$DEST/"
    fi

# Build assembly jars
assembly:
    sbt +assembly

# Clean build artifacts
clean: clean-headers
    sbt clean cleanFiles
    cd native && cargo clean

# Run tests
test:
    sbt +test

# Generate documentation
site:
    sbt makeSite

# Publish documentation
publish-site:
    sbt ghpagesPushSite

# Release artifacts
release:
    sbt ci-release
