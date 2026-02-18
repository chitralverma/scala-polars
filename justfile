set ignore-comments := true

root := justfile_directory()
native_root := root / 'native'
native_manifest := native_root / 'Cargo.toml'
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
    @just echo-command 'Formatting core module'
    @sbt -error scalafmtAll scalafmtSbt javafmtAll reload
    @just echo-command 'Formatting native module'
    @cargo clippy -q {{ cargo_flags }} --no-deps --fix --allow-dirty --allow-staged --manifest-path {{ native_manifest }}
    @cargo sort {{ native_root }}
    @cargo fmt --quiet --manifest-path {{ native_manifest }}
    @just --fmt --unstable

# Check formatting and linting
[group('lint')]
lint:
    @just echo-command 'Checking core module'
    @sbt -error scalafmtCheckAll scalafmtSbtCheck javafmtCheckAll
    @just echo-command 'Checking native module'
    @cargo clippy -q {{ cargo_flags }} --no-deps --manifest-path {{ native_manifest }} -- -D warnings
    @cargo sort {{ native_root }} --check
    @cargo fmt --check --manifest-path {{ native_manifest }}
    @just --fmt --unstable --check

# Run all code formatting and quality checks
[group('lint')]
pre-commit: fmt lint

# Generate JNI headers
[group('dev')]
gen-headers:
    @just echo-command 'Generating JNI headers'
    @sbt genHeaders

# Build native library TARGET_TRIPLE, NATIVE_RELEASE, NATIVE_LIB_LOCATION env vars are supported
[group('dev')]
build-native:
    @just echo-command 'Building native library'
    @sbt generateNativeLibrary

# Build assembly jars
[group('dev')]
assembly:
    @sbt +assembly

# Compile
[group('dev')]
compile:
    @sbt compile

# Clean build artifacts
[group('dev')]
clean:
    @just echo-command 'Cleaning native artifacts'
    @cargo clean --manifest-path {{ native_manifest }} --quiet
    @just echo-command 'Cleaning JNI headers'
    @sbt -error cleanHeaders
    @just echo-command 'Cleaning core artifacts'
    @sbt -error clean cleanFiles reload

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

# Release/publish artifacts locally
[group('release')]
release-local:
    @sbt publishLocal
