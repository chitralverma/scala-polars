scala-polars
============

`scala-polars` is a library for using the awesome [Polars](https://www.pola.rs/) DataFrame library in
Scala and Java projects.

## About

### About Polars

Polars is a blazing fast DataFrames library implemented in Rust using
[Apache Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html) as the memory model.

- Lazy / eager execution
- Multithreaded
- SIMD
- Query optimization
- Powerful expression API
- Hybrid Streaming (larger than RAM datasets)
- Rust | Python | NodeJS | ...

### About scala-polars

This library has been written mostly in scala and leverages [JNI](https://en.wikipedia.org/wiki/Java_Native_Interface)
to offload heavy data processing tasks to its native counterpart written completely in rust. The aim of this library is
to provide an easy-to-use interface for Scala/ Java developers though which they can leverage the amazing Polars library
in their existing projects.

The project is mainly divided into 2 submodules,

- `core` - Contains the user facing interfaces written in scala that will be used to work with data. Internally this
  module relies on native submodule.
- `native` - This is an internal module written in rust which relies on the official rust implementation of Polars.

### Examples

- [Java Examples](examples/src/main/java/examples/java/)
- [Scala Examples](examples/src/main/scala/examples/scala/)

## Compatibility

- JDK version `>=8`
- Scala version `2.12.x`, `2.13.x` and `3.3.x`. Default is `2.13.x`
- Rust version `>=1.58`

## Building

### Prerequisites

The following tooling is required to start building `scala-polars`,

- JDK 8+ ([OpenJDK](https://openjdk.org/projects/jdk/)
  or [Oracle Java SE](https://www.oracle.com/java/technologies/javase/))
- [Rust](https://www.rust-lang.org/tools/install) (cargo, rustc etc.)
- [sbt](https://www.scala-sbt.org/index.html)

### How to Compile?

sbt is the primary build tool for this project and all the required interlinking has been done in such a way that your
IntelliJ IDE or an external build works in the same way. This means that whether you are in development mode or want to
build to distribute, the process of the build remains the same and is more or less abstracted.

The build process that sbt triggers involves the following steps,

- Compile the rust code present in the `native` module to a binary.
- Compile the scala and java (if any) facade code.
- Copy the built rust binary to the classpath of scala code during its build at a fixed location.

All of the above steps happen automatically when you run an sbt build job that triggers `compile` phase. Other than
this, during package phase, the scala, java code and the built rust binary is added to the built jar(s). To keep
everything monolithic, the `native` module is not packaged as a jar, only `core` module is.

The above process might look complicated, and it actually is 😂, but since all the internally sbt wiring is already in
place, the user facing process is fairly straight-forward. This can be done by going through the following steps in
sequence firstly ensure JDK 8+, sbt and the latest rust
compiler are installed, then follow the commands below as per the need.

**Compilation**

```shell
# To compile the whole project (scala/ java/ rust) in one go
sbt compile
```

**Local packaging/ installation**

```shell
# To package the project and install locally as slim jars with default scala version.
sbt publishLocal

# To package the project and install locally as slim jars for all supported scala versions.
sbt +publishLocal
```

**Build Assembly (fat jar)**

```shell
# To package the project and install locally as fat jars with default scala version.
sbt assembly

# To package the project and install locally as slim jars for all supported scala versions.
sbt +assembly
```

**Generate Native Binary Only**

```shell
# To compile only the native module containing rust code to binary.
sbt generateNativeLibrary
```

## License

Apache License 2.0, see [LICENSE](LICENSE).

## Community

Reach out to the Polars community on [Discord](https://discord.gg/4UfP5cfBE7).
