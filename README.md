# scala-polars

**`scala-polars`** brings the blazing-fast [Polars](https://www.pola.rs/) DataFrame library to Scala and Java projects.

---

## 🚀 Overview

Polars is a lightning-fast DataFrame library built in Rust using
the [Apache Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html). `scala-polars` bridges the gap
between the JVM and Polars by exposing it through a JNI-based Scala API, allowing developers to process data with native
performance in a fully JVM-compatible way.

---

## 🔍 Features

- **Native performance**: backed by Polars' highly optimized Rust core
- **Seamless Scala/Java integration** via JNI
- **Lazy & eager execution modes**
- **Multithreaded & SIMD-accelerated**
- **Memory-efficient**: handles out-of-core datasets
- **Works out of the box** using SBT (includes native build automation)

---

## 📦 Installation

### SBT

```scala
resolvers += Resolver.sonatypeCentralSnapshots

libraryDependencies += "com.github.chitralverma" %% "scala-polars" % "SOME-VERSION-SNAPSHOT"
```

> 💡 Find the latest snapshot versions
> on [Sonatype Central](https://central.sonatype.com/service/rest/repository/browse/maven-snapshots/com/github/chitralverma/scala-polars_2.12/)

### Maven

```xml
<repositories>
    <repository>
        <name>Central Portal Snapshots</name>
        <id>central-portal-snapshots</id>
        <url>https://central.sonatype.com/repository/maven-snapshots/</url>
        <releases>
            <enabled>false</enabled>
        </releases>
        <snapshots>
            <enabled>true</enabled>
            <updatePolicy>always</updatePolicy>
        </snapshots>
    </repository>
</repositories>

<dependencies>
    ...
    <dependency>
        <groupId>com.github.chitralverma</groupId>
        <artifactId>scala-polars_2.12</artifactId>
        <version>SOME-VERSION-SNAPSHOT</version>
    </dependency>
    ...
</dependencies>
```

### Gradle

```groovy
repositories {
    maven {
        name = 'Central Portal Snapshots'
        url = 'https://central.sonatype.com/repository/maven-snapshots/'

        // Only search this repository for the specific dependency
        content {
            includeModule("com.github.chitralverma", "scala-polars_2.12")
        }
    }
    mavenCentral()
}
implementation("com.github.chitralverma:scala-polars_2.12:SOME-VERSION-SNAPSHOT")
```

> Note: Use `scala-polars_2.13` for Scala 2.13.x projects or `scala-polars_3` for Scala 3.x projects as the artifact ID

---

## 🧱 Modules

- `core`: Scala interface users directly interact with
- `native`: Rust backend that embeds Polars and is compiled into a JNI shared library

---

## 🧪 Getting Started

### Scala

```scala
import com.github.chitralverma.polars.api.{DataFrame, Series}

val df = DataFrame
  .fromSeries(
    Series.ofInt("i32_col", Array[Int](1, 2, 3)),
    Series.ofLong("i64_col", Array[Long](1L, 2L, 3L)),
    Series.ofBoolean("bool_col", Array[Boolean](true, false, true)),
    Series.ofList(
      "nested_str_col",
      Array[Array[String]](Array("a", "b", "c"), Array("a", "b", "c"), Array("a", "b", "c"))
    )
  )

val result = df.select("i32_col", "i64_col")
result.show()
```

### Java

```java
import com.github.chitralverma.polars.api.DataFrame;
import com.github.chitralverma.polars.api.Series;

DataFrame df = DataFrame.fromSeries(
    Series.ofInt("i32_col", new int[] {1, 2, 3}),
    Series.ofLong("i64_col", new long[] {1L, 2L, 3L}),
    Series.ofBoolean("bool_col", new boolean[] {true, false, true}),
    Series.ofList(
        "nested_str_col",
        new String[][] {
                {"a", "b", "c"},
                {"a", "b", "c"},
                {"a", "b", "c"},
        }
    )
)
.select("i32_col", "i64_col");

df.show();
```

👉 See full:

- [Scala Examples](examples/src/main/scala/examples/scala/)
- [Java Examples](examples/src/main/java/examples/java/)

---

## 🔧 Compatibility

- **Scala**: 2.12, 2.13, 3.x
- **Java**: 8+
- **Rust**: 1.58+
- **OS**: macOS, Linux, Windows

---

## 🏗 Build from Source

### Requirements

- JDK 8+
- [Rust](https://www.rust-lang.org/tools/install)
- [sbt](https://www.scala-sbt.org/)
- [just](https://github.com/casey/just)

### Commands

```bash
# Compile Rust + Scala + Java
just compile

# Publish locally
just release-local

# Fat JAR (default Scala version)
just assembly

# Rust native only
just build-native

# Rust native only (Release profile)
NATIVE_RELEASE=true just build-native
```

---

## 📄 License

Apache 2.0 — see [LICENSE](LICENSE)

---

## 🤝 Community

- Discuss Polars on [Polars Discord](https://discord.gg/4UfP5cfBE7)
- To contribute, see [CONTRIBUTING.md](CONTRIBUTING.md)
