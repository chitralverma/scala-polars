package com.github.chitralverma.polars

import com.github.chitralverma.polars.functions._
import com.github.chitralverma.polars.testing.PolarsTestBase

/** Harness self-test (Scala): native lib loads under `Test`, a frame round-trips an expression.
  * Not a port of an upstream pytest; the Java mirror is `HarnessSmokeTest`.
  */
class HarnessSmokeSuite extends PolarsTestBase {

  test("native library loads and reports a version") {
    val version = Polars.version()
    version should not be null
    version.trim should not be empty
  }

  test("build a frame, apply col(a) + 1, collect, and read values back") {
    val df = intFrame("a", 1, 2, 3)
    val result = df.withColumn("b", col("a") + 1)

    assertRowCount(result, 3)
    assertColumns(result, "a", "b")
    assertColumnValues(result, "b", 2, 3, 4)
  }

  test("filter keeps only matching rows") {
    val df = intFrame("a", 1, 2, 3, 4)
    val result = df.filter(col("a") > 2)

    assertRowCount(result, 2)
    assertColumnValues(result, "a", 3, 4)
  }
}
