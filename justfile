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
    @sbt --batch -error scalafmtAll scalafmtSbt javafmtAll reload
    @just echo-command 'Formatting native module'
    @cargo clippy -q {{ cargo_flags }} --no-deps --fix --allow-dirty --allow-staged --manifest-path {{ native_manifest }}
    @cargo sort {{ native_root }}
    @cargo fmt --quiet --manifest-path {{ native_manifest }}
    @just --fmt --unstable

# Check formatting and linting
[group('lint')]
lint:
    @just echo-command 'Checking core module'
    @sbt --batch -error scalafmtCheckAll scalafmtSbtCheck javafmtCheckAll
    @just echo-command 'Checking test-source docs (Test/doc; caught by ci-release)'
    @sbt --batch -error "scala-polars/Test/doc"
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
    @sbt --batch genHeaders

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
    @just echo-command 'Cleaning JNI headers'
    @sbt --batch -error cleanHeaders
    @just echo-command 'Cleaning core artifacts'
    @sbt --batch -error clean cleanFiles reload

# Run tests (Scala + Java); pass a Scala version to scope, e.g. `just test 2.13.18`
[group('dev')]
test scala_version='':
    @just echo-command 'Running tests'
    @sbt --batch {{ if scala_version == '' { '+test' } else { '"++' + scala_version + ' scala-polars/test"' } }}

# Run tests reusing an already-built native lib (skips the Rust rebuild); requires `just build-native` first
[group('dev')]
test-fast scala_version='':
    #!/usr/bin/env bash
    set -euo pipefail
    arch="$(rustc -vV | sed -n 's/^host: //p' | cut -d- -f1)"
    libdir="$(find core/target native/target -type f \
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
    {{ if scala_version == '' { 'sbt --batch +test' } else { 'sbt --batch "++' + scala_version + ' scala-polars/test"' } }}

# Generate Scala coverage reports (sbt-scoverage); reuses an existing native lib. Pass a Scala version to scope.
[group('dev')]
coverage scala_version='':
    #!/usr/bin/env bash
    set -euo pipefail
    arch="$(rustc -vV | sed -n 's/^host: //p' | cut -d- -f1)"
    libdir="$(find core/target native/target -type f \
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
    just echo-command 'Running Scala coverage'
    ver='{{ scala_version }}'
    if [[ -z "${ver}" ]]; then
        sbt --batch coverage "scala-polars/test" "scala-polars/coverageReport"
    else
        sbt --batch "++${ver}" coverage "scala-polars/test" "scala-polars/coverageReport"
    fi
    echo "Scala coverage report (HTML): core/target/scala-*/scoverage-report/index.html"
    echo "Cobertura XML (for Codecov): core/target/scala-*/coverage-report/cobertura.xml"
    echo "For Java coverage (JSeries.java) run 'just coverage-java'."

# Generate Java coverage (sbt-jacoco) for the Java production source; reuses an existing native lib.
[group('dev')]
coverage-java scala_version='2.13.18':
    #!/usr/bin/env bash
    set -euo pipefail
    arch="$(rustc -vV | sed -n 's/^host: //p' | cut -d- -f1)"
    libdir="$(find core/target native/target -type f \
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
    just echo-command 'Running Java coverage (JaCoCo)'
    sbt --batch "++{{ scala_version }}" "scala-polars/jacoco"
    report="$(find core/target -path '*/jacoco/report/jacoco.xml' 2>/dev/null | head -1)"
    echo "Java coverage report (HTML): ${report%/jacoco.xml}/html/index.html"
    echo "JaCoCo XML (for Codecov): ${report}"

# Generate documentation
[group('release')]
site:
    @sbt --batch makeSite

# Publish documentation
[group('release')]
publish-site:
    @sbt --batch ghpagesPushSite

# Release artifacts
[group('release')]
release:
    @sbt --batch ci-release

# Release/publish artifacts locally
[group('release')]
release-local:
    @sbt --batch publishLocal
