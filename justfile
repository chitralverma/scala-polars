set ignore-comments

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
    @sbt --batch -error "scalafmtAll ; scalafmtSbt ; javafmtAll ; reload"
    @just echo-command 'Formatting native module'
    @cargo clippy -q {{ cargo_flags }} --no-deps --fix --allow-dirty --allow-staged --manifest-path {{ native_manifest }}
    @cargo sort {{ native_root }}
    @cargo fmt --quiet --manifest-path {{ native_manifest }}
    @just --fmt --unstable

# Check formatting and linting
[group('lint')]
lint:
    @just echo-command 'Checking core module'
    @sbt --batch -error "scalafmtCheckAll ; scalafmtSbtCheck ; javafmtCheckAll"
    @just echo-command 'Checking native module'
    @cargo clippy -q {{ cargo_flags }} --no-deps --manifest-path {{ native_manifest }} -- -D warnings
    @cargo sort {{ native_root }} --check
    @cargo fmt --check --manifest-path {{ native_manifest }}
    @just --fmt --unstable --check

# Check API doc generation across all Scala versions (skip-guarded; mirrors ci-release's doc)
[group('lint')]
check-docs:
    @just echo-command 'Checking API docs (Compile + Test) across Scala versions'
    @SKIP_NATIVE_GENERATION=true sbt --batch "+scala-polars/Compile/doc ; +scala-polars/Test/doc"

# Run all code formatting and quality checks
[group('lint')]
pre-commit: fmt lint check-docs

# Build native library TARGET_TRIPLE, NATIVE_RELEASE, NATIVE_LIB_LOCATION env vars are supported
[group('dev')]
build-native:
    @just echo-command 'Building native library'
    @sbt --batch generateNativeLibrary

# Build assembly jars
[group('dev')]
assembly:
    @sbt --batch +assembly

# Compile
[group('dev')]
compile:
    @sbt --batch compile

# Clean build artifacts
[group('dev')]
clean:
    @just echo-command 'Cleaning native artifacts'
    @cargo clean --manifest-path {{ native_manifest }} --quiet
    @just echo-command 'Cleaning core artifacts'
    @sbt --batch -error "clean ; cleanFiles ; reload"

# Run tests (Scala + Java); pass a Scala version to scope, e.g. `just test 2.13.18`
[group('dev')]
test scala_version='':
    @just echo-command 'Running tests'
    @sbt --batch {{ if scala_version == '' { '+testFull' } else { '"++' + scala_version + ' ; scala-polars/testFull"' } }}

# Run tests reusing an already-built native lib (skips the Rust rebuild); requires `just build-native` first
[group('dev')]
test-fast scala_version='':
    #!/usr/bin/env bash
    set -euo pipefail
    arch="$(rustc -vV | sed -n 's/^host: //p' | cut -d- -f1)"
    libdir="$(find target/out native/target -type f \
        \( -name 'libscala_polars.so' -o -name 'libscala_polars.dylib' -o -name 'scala_polars.dll' \) \
        2>/dev/null | head -1)"
    if [[ -z "${libdir}" ]]; then
        echo "No built native library found. Run 'just build-native' first." >&2
        exit 1
    fi
    staged="$(mktemp -d)/native-libs"
    mkdir -p "${staged}/${arch}"
    cp "${libdir}" "${staged}/${arch}/"
    echo "Reusing native lib '${libdir}' (arch: ${arch})"
    export SKIP_NATIVE_GENERATION=true
    export NATIVE_LIB_LOCATION="${staged}"
    just echo-command 'Running tests (reusing native lib)'
    {{ if scala_version == '' { 'sbt --batch +testFull' } else { 'sbt --batch "++' + scala_version + ' ; scala-polars/testFull"' } }}

# Generate Scala coverage reports (sbt-scoverage); reuses an existing native lib. Pass a Scala version to scope.
[group('dev')]
coverage scala_version='':
    #!/usr/bin/env bash
    set -euo pipefail
    arch="$(rustc -vV | sed -n 's/^host: //p' | cut -d- -f1)"
    libdir="$(find target/out native/target -type f \
        \( -name 'libscala_polars.so' -o -name 'libscala_polars.dylib' -o -name 'scala_polars.dll' \) \
        2>/dev/null | head -1)"
    if [[ -z "${libdir}" ]]; then
        echo "No built native library found. Run 'just build-native' first." >&2
        exit 1
    fi
    staged="$(mktemp -d)/native-libs"
    mkdir -p "${staged}/${arch}"
    cp "${libdir}" "${staged}/${arch}/"
    export SKIP_NATIVE_GENERATION=true
    export NATIVE_LIB_LOCATION="${staged}"
    # Bypass sbt 2.x global cache with a temporary cache directory to force scoverage instrumentation.
    tmp_cache="$(mktemp -d)/sbt-cache"
    just echo-command "Running Scala coverage (cache: ${tmp_cache})"
    ver='{{ scala_version }}'
    if [[ -z "${ver}" ]]; then
        sbt -Dsbt.global.localcache="${tmp_cache}" --batch "clean ; coverage ; scala-polars/testFull ; scala-polars/coverageReport"
    else
        sbt -Dsbt.global.localcache="${tmp_cache}" --batch "++${ver} ; clean ; coverage ; scala-polars/testFull ; scala-polars/coverageReport"
    fi
    echo "Scala coverage report (HTML): target/out/jvm/scala-*/scala-polars/scoverage-report/index.html"
    echo "Cobertura XML (for Codecov): target/out/jvm/scala-*/scala-polars/coverage-report/cobertura.xml"
    echo "For Java coverage (JSeries.java) run 'just coverage-java'."

# Generate Java coverage (sbt-jacoco) for the Java production source; reuses an existing native lib.
[group('dev')]
coverage-java scala_version='2.13.18':
    #!/usr/bin/env bash
    set -euo pipefail
    arch="$(rustc -vV | sed -n 's/^host: //p' | cut -d- -f1)"
    libdir="$(find target/out native/target -type f \
        \( -name 'libscala_polars.so' -o -name 'libscala_polars.dylib' -o -name 'scala_polars.dll' \) \
        2>/dev/null | head -1)"
    if [[ -z "${libdir}" ]]; then
        echo "No built native library found. Run 'just build-native' first." >&2
        exit 1
    fi
    staged="$(mktemp -d)/native-libs"
    mkdir -p "${staged}/${arch}"
    cp "${libdir}" "${staged}/${arch}/"
    export SKIP_NATIVE_GENERATION=true
    export NATIVE_LIB_LOCATION="${staged}"
    # Bypass sbt 2.x global cache with a temporary cache directory to force jacoco instrumentation.
    tmp_cache="$(mktemp -d)/sbt-cache"
    just echo-command "Running Java coverage (JaCoCo) (cache: ${tmp_cache})"
    sbt -Dsbt.global.localcache="${tmp_cache}" -Djacoco.enable=true --batch "++{{ scala_version }} ; clean ; scala-polars/jacoco"
    report="$(find target/out -path '*/jacoco/report/jacoco.xml' 2>/dev/null | head -1)"
    echo "Java coverage report (HTML): ${report%/jacoco.xml}/html/index.html"
    echo "JaCoCo XML (for Codecov): ${report}"

# Generate API documentation (Scaladoc + Javadoc) for the core module
[group('release')]
site:
    @sbt --batch "scala-polars/Compile/doc"

# Release artifacts
[group('release')]
release:
    @sbt --batch ci-release

# Release/publish artifacts locally
[group('release')]
release-local:
    @sbt --batch publishLocal
