# scala-polars

[![Build Status](https://github.com/chitralverma/scala-polars/actions/workflows/release.yml/badge.svg)](https://github.com/chitralverma/scala-polars/actions)
[![Maven Central Snapshots](https://img.shields.io/nexus/s/com.github.chitralverma/scala-polars_2.13?server=https%3A%2F%2Fcentral.sonatype.com)](https://central.sonatype.com)
[![License](https://img.shields.io/github/license/chitralverma/scala-polars)](LICENSE)
[![Discord](https://img.shields.io/discord/4UfP5cfBE7?logo=discord&logoColor=white)](https://discord.gg/4UfP5cfBE7)

`scala-polars` brings the blazing-fast, memory-efficient [Polars](https://www.pola.rs/) DataFrame library to the JVM (Scala and Java) via high-performance JNI bindings.

---

## Architecture Overview

Traditional JVM data libraries often suffer from garbage collection (GC) overhead and intensive object-serialization costs. `scala-polars` bypasses these limitations by leveraging off-heap native memory layouts using the **Apache Arrow Columnar Format** via modern, thread-safe JNI bridges.

```text
  ┌─────────────────────────────────────────────────────────────┐
  │                   JVM (Scala / Java)                        │
  │  - Low GC Pressure   - Type-safe API   - Lazy Plans         │
  └──────────────────────────────┬──────────────────────────────┘
                                 │ Direct JNI Bridge
                                 ▼ (jni-rs 0.22)
  ┌─────────────────────────────────────────────────────────────┐
  │                  Rust Core (scala-polars-native)            │
  │  - Polars 0.54.x     - SIMD Vectors    - Query Optimizer    │
  └──────────────────────────────┬──────────────────────────────┘
                                 │ Zero-Copy Off-Heap Reference
                                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │                 Apache Arrow Memory Layout                  │
  └─────────────────────────────────────────────────────────────┘
```

---

## Core Features

- **Blazing Native Performance:** Backed by Polars' highly optimized Rust engine, SIMD-accelerated execution, and cache-coherent vector layouts.
- **Zero-Copy Memory Design:** Shares underlying tabular data natively using Arrow buffers without copying across the JVM/Native boundary wherever possible.
- **Expressive Lazy Evaluation:** Build complex queries lazily. The Rust engine optimizes the logical query plan (projection pushdown, predicate pushdown, type coercion, and limit pushdown) before executing natively on the hardware.
- **Out-of-Core Processing:** Stream datasets larger than available system memory using lazy file scanners.
- **Out-of-the-Box Multiplatform Support:** Distributes as a single "Fat JAR" containing pre-compiled binaries for 6 major platforms:
  - **Linux** (`x86_64`, `AArch64`)
  - **macOS** (`x86_64`, `Apple Silicon/M-series`)
  - **Windows** (`x86_64`, `ARM64`)
- **Zero Configuration:** The library automatically detects, extracts, and loads the correct native binary for your architecture on startup.

---

## Installation

### SBT

To use snapshot builds from Sonatype Central, append the resolver and declare the library dependency matching your Scala version:

```scala
resolvers += Resolver.sonatypeCentralSnapshots

libraryDependencies += "com.github.chitralverma" %% "scala-polars" % "0.1.0-SNAPSHOT"
```

### Maven

Configure the snapshot repository and add the dependency coordinates (substituting `_2.13` with `_2.12` or `_3` depending on your Scala version):

```xml
<repositories>
    <repository>
        <id>central-portal-snapshots</id>
        <name>Central Portal Snapshots</name>
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
    <dependency>
        <groupId>com.github.chitralverma</groupId>
        <artifactId>scala-polars_2.13</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

### Gradle

```groovy
repositories {
    maven {
        name = 'Central Portal Snapshots'
        url = 'https://central.sonatype.com/repository/maven-snapshots/'
        content {
            includeModule("com.github.chitralverma", "scala-polars_2.13")
        }
    }
    mavenCentral()
}

implementation("com.github.chitralverma:scala-polars_2.13:0.1.0-SNAPSHOT")
```

---

## Getting Started

### 1. Lazy Execution API (Recommended)

Lazy execution optimizes queries globally before loading or transforming data. This example lazily scans a CSV, filters rows, selects columns, sorts, and limits the result.

```scala
import com.github.chitralverma.polars.Polars
import com.github.chitralverma.polars.api._
import com.github.chitralverma.polars.functions._

// 1. Define a lazy computation plan (no data is read or loaded yet)
val lazyPlan = Polars.scan
  .csv("employee_data.csv")
  .filter(col("age") >= lit(21))
  .select(col("name"), col("department"), col("salary"))
  .sort("salary", descending = true, nullLast = false, maintainOrder = false)
  .limit(5)

// 2. Compile, optimize, and execute the plan natively in Rust
val result: DataFrame = lazyPlan.collect()

// 3. Render the output
result.show()
```

### 2. Eager Execution API

For quick interactive sessions, dataframes can be manipulated eagerly.

```scala
import com.github.chitralverma.polars.api.{DataFrame, Series}

val df = DataFrame.fromSeries(
  Series.ofInt("i32_col", Array(1, 2, 3)),
  Series.ofLong("i64_col", Array(1L, 2L, 3L)),
  Series.ofBoolean("bool_col", Array(true, false, true))
)

val filteredDf = df.select("i32_col", "bool_col")
filteredDf.show()
```

### 3. Java Interoperability

`scala-polars` provides first-class support for Java projects:

```java
import com.github.chitralverma.polars.api.DataFrame;
import com.github.chitralverma.polars.api.Series;

DataFrame df = DataFrame.fromSeries(
    Series.ofInt("i32_col", new int[] {1, 2, 3}),
    Series.ofLong("i64_col", new long[] {1L, 2L, 3L}),
    Series.ofBoolean("bool_col", new boolean[] {true, false, true})
)
.select("i32_col", "bool_col");

df.show();
```

---

## Platform Support

| Operating System | Architecture | Build Support |
| :--- | :--- | :--- |
| **Linux** | `x86_64` / `AArch64` | Native (glibc 2.35+) |
| **macOS** | `x86_64` / `Apple Silicon` | Native (macOS 12+) |
| **Windows** | `x86_64` / `ARM64` | Native MSVC |

---

## Building From Source

To compile, build, or run the library from source locally, please refer to our [Contributing Guide](CONTRIBUTING.md) for full system prerequisites and available build commands.

---

## License

`scala-polars` is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for more details.

---

## Community & Contributing

- Join the discussion on the official [Polars Discord](https://discord.gg/4UfP5cfBE7).
- To contribute code, report issues, or suggest improvements, please check out [CONTRIBUTING.md](CONTRIBUTING.md).
