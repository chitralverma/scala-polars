# Contributing to scala-polars

Thank you for your interest in contributing to `scala-polars`!

Whether you are fixing a bug, writing tests, improving documentation, or adding new features, your contributions are highly valued.

---

## Project Structure

- `core/`: The main Scala and Java API used by end users.
- `native/`: The Rust module compiled into a shared library via Cargo and linked via JNI.
- `examples/`: Sample applications in both Scala and Java.

---

## Build Prerequisites

Ensure the following tools are installed on your system before building:

- **JDK 17+**
- **Rust** (nightly toolchain)
- **sbt** (2.x)
- **just** (command runner)

---

## Compilation Process

`sbt` drives the entire build process. The native Rust compilation is fully integrated with the JVM compiler task flow, abstracting the multi-language build pipeline so that both IDEs and command-line builds function identically.

When compiling:
1. The Rust code in the `native` module compiles to a platform-specific binary.
2. The Scala and Java API facade code is compiled.
3. The built native shared library is copied to the classpath of the Scala code at a fixed target location.

These steps happen automatically when triggering any sbt compilation. When packaging or assembling, the native libraries for all platforms are bundled into a single "Fat JAR" coordinate within the `core` module, simplifying dependency resolution for end users.

To compile and build successfully, ensure that JDK 17+, sbt, and the correct Rust toolchain are active, then execute the commands below as needed.

---

## Build Commands

### Full Project (Rust + Scala/Java)

```bash
just compile
```

### Native Rust Library Only

```bash
just build-native
```

### Native Rust Library Only (Release Profile)

```bash
NATIVE_RELEASE=true just build-native
```

### Local Publication

```bash
just release-local
```

### Create Assembly Fat JAR

```bash
just assembly
```

---

## Testing

Ensure the unit and smoke test suites pass before submitting changes:

```bash
just test
```

---

## Code Conventions

- **Idiomatic Code:** Follow standard Scala and Java coding patterns.
- **Synchronized Interfaces:** Ensure that JVM `@native` definitions and Rust entry points remain strictly aligned.
- **Cross-Compilation:** Validate public API changes across all cross-built Scala versions (`2.12`, `2.13`, `3.x`).
- **Pristine Documentation:** Accompany any new public classes or methods with clean, comprehensive docstrings. Avoid conversational, informal, or transitional remarks.

---

## Publishing Snapshots (Maintainers Only)

To publish a snapshot build to Sonatype Central:

```bash
just release
```

---

## Getting Help

- [Open a GitHub Issue](https://github.com/chitralverma/scala-polars/issues)
- Join the [Polars Discord](https://discord.gg/4UfP5cfBE7)
