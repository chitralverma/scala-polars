package com.github.chitralverma.polars

import com.github.chitralverma.polars.functions._
import com.github.chitralverma.polars.testing.PolarsTestBase

/** Replicates behaviours tested in upstream py-polars
  * `tests/unit/operations/namespaces/test_name.py` and asserts column aliasing (`alias`/`as`)
  * behaviour.
  */
class NameSuite extends PolarsTestBase {

  test("expression alias and JVM-friendly as") {
    val df = intFrame("ColX", 1, 2, 3)

    // Test alias
    val dfAlias = df.select(col("ColX").alias("AliasX"))
    dfAlias.schema.getField("AliasX") shouldBe defined
    dfAlias.schema.getField("ColX") should not be defined
    assertColumnValues(dfAlias, "AliasX", 1, 2, 3)

    // Test alias as
    val dfAs = df.select(col("ColX").as("AsX"))
    dfAs.schema.getField("AsX") shouldBe defined
    dfAs.schema.getField("ColX") should not be defined
    assertColumnValues(dfAs, "AsX", 1, 2, 3)

    // Test aliasing non-Column Expressions (e.g. literals)
    val dfLit = df.select(col("ColX"), lit(42).alias("LitX"))
    dfLit.schema.getField("LitX") shouldBe defined
    assertColumnValues(dfLit, "LitX", 42, 42, 42)
  }

  test("name namespace: change case (toUppercase / toLowercase)") {
    val df = intFrame("ColX", 1, 2, 3)

    // Test toUppercase
    val dfUpper = df.select(col("ColX").name.toUppercase)
    dfUpper.schema.getField("COLX") shouldBe defined
    dfUpper.schema.getField("ColX") should not be defined
    assertColumnValues(dfUpper, "COLX", 1, 2, 3)

    // Test toLowercase
    val dfLower = df.select(col("ColX").name.toLowercase)
    dfLower.schema.getField("colx") shouldBe defined
    dfLower.schema.getField("ColX") should not be defined
    assertColumnValues(dfLower, "colx", 1, 2, 3)
  }

  test("name namespace: prefix and suffix") {
    val df = intFrame("ColX", 1, 2, 3)

    // Test prefix
    val dfPrefix = df.select(col("ColX").name.prefix("pre_"))
    dfPrefix.schema.getField("pre_ColX") shouldBe defined
    dfPrefix.schema.getField("ColX") should not be defined
    assertColumnValues(dfPrefix, "pre_ColX", 1, 2, 3)

    // Test suffix
    val dfSuffix = df.select(col("ColX").name.suffix("_post"))
    dfSuffix.schema.getField("ColX_post") shouldBe defined
    dfSuffix.schema.getField("ColX") should not be defined
    assertColumnValues(dfSuffix, "ColX_post", 1, 2, 3)
  }

  test("name namespace: keep") {
    val df = intFrame("ColX", 1, 2, 3)

    // keep preserves the original name of the column
    val dfKeep = df.select(col("ColX").name.keep)
    dfKeep.schema.getField("ColX") shouldBe defined
    assertColumnValues(dfKeep, "ColX", 1, 2, 3)
  }

}
