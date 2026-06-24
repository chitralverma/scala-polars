# Contributing to scala-polars

Thanks for your interest in contributing to `scala-polars`! 🚀

Whether you're fixing a bug, writing tests/ documentation, or building features, we welcome your help.

---

## 🛠️ Project Structure

- `core/`: The main Scala and Java API used by end users
- `native/`: A Rust module compiled into a shared library via Cargo and linked via JNI
- `examples/`: Sample applications in both Scala and Java

---

## 💧 Build Prerequisites

Ensure the following are installed:

- Java 8+ (JDK)
- [Rust](https://www.rust-lang.org/tools/install)
- [sbt](https://www.scala-sbt.org/)
- [just](https://github.com/casey/just)

---

## Compilation Process

`sbt` is the primary build tool for this project, and all the required interlinking has been done in such a way that
your
IntelliJ IDE or an external build works in the same way. This means that whether you are in development mode or want to
build to distribute, the process of the build remains the same and is more or less abstracted.

The build process that sbt triggers involves the following steps

- Compile the rust code present in the `native` module to a binary.
- Compile the scala and java (if any) facade code.
- Copy the built rust binary to the classpath of scala code during its build at a fixed location.

All the above steps happen automatically when you run an sbt build job that triggers `compile` phase. Other than
this, during the package phase, the scala, java code and the built rust binary are added to the built jar(s). To keep
everything monolithic, the `native` module is not packaged as a jar, only `core` module is.

The above process might look complicated, and it actually is 😂, but since all the internally sbt wiring is already in
place, the user facing process is fairly straight-forward. This can be done by going through the following steps in
sequence firstly ensure JDK 8+, sbt and the latest rust
compiler are installed, then follow the commands below as per the need.

---

## 🏗 Build Commands

### Full project (Rust + Scala/Java)

```bash
just compile
```

### Native Rust library only

```bash
just build-native
```

### Native Rust library only (Release profile)

```bash
NATIVE_RELEASE=true just build-native
```

### Locally publish

```bash
just release-local
```

### Fat JAR

```bash
just assembly
```

---

## 🔍 Testing

Run unit tests via:

```bash
just test
```

---

## ✍️ Conventions

- Follow idiomatic Scala where possible.
- Keep Rust and Scala types in sync when modifying the native interface.
- Test across multiple Scala versions (`2.12`, `2.13`, `3.x`) if making public API changes.
- Document any native API changes clearly.

---

## 📤 Publishing Snapshots

If you're a maintainer:

```bash
# Publish to Sonatype snapshots
just release
```

---

## 📬 Getting Help

- [Open an issue](https://github.com/chitralverma/scala-polars/issues)
- Join the [Polars Discord](https://discord.gg/4UfP5cfBE7)

We appreciate every contribution ❤️
