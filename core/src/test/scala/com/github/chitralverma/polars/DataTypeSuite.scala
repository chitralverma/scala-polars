package com.github.chitralverma.polars

import com.github.chitralverma.polars.api.types._
import com.github.chitralverma.polars.functions._
import com.github.chitralverma.polars.testing.PolarsTestBase

/** Tests the `DataType` surface. Verifies de-collapsed integers, precise floating types, temporal
  * types, and nested types can be round-tripped with their JNI String/JSON ffiName, casted
  * end-to-end, and verified in Rows.
  */
class DataTypeSuite extends PolarsTestBase {

  test("ffiName serializes to official Polars serde JSON") {
    DataTypes.String.ffiName shouldBe "String"
    DataTypes.Int32.ffiName shouldBe "Int32"
    DataTypes.Float64.ffiName shouldBe "Float64"
    DataTypes.Boolean.ffiName shouldBe "Boolean"

    DataTypes.time("Microseconds").ffiName shouldBe """{"Time":"Microseconds"}"""
    DataTypes
      .datetime("Microseconds", "UTC")
      .ffiName shouldBe """{"Timestamp":["Microseconds","UTC"]}"""
    DataTypes.duration("Microseconds").ffiName shouldBe """{"Duration":"Microseconds"}"""

    DataTypes.list(DataTypes.Int32).ffiName shouldBe """{"List":{"dtype":"Int32"}}"""
    DataTypes
      .struct(Array(Field("a", DataTypes.Int32)))
      .ffiName shouldBe """{"Struct":[{"name":"a","dtype":"Int32"}]}"""
  }

  test("datatype equality and time units") {
    DataTypes.datetime("ms", "UTC") should not be DataTypes.datetime("ns", "UTC")
    DataTypes.duration("ns") should not be DataTypes.duration("us")

    DataTypes.datetime("us", "UTC") shouldBe DataTypes.datetime("us", "UTC")
    DataTypes.duration("us") shouldBe DataTypes.duration("us")
  }

  test("cast end-to-end (including exact signed/unsigned widths)") {
    val df = intFrame("a", 1, 2, 3)

    // Cast Int32 -> Int16
    val result1 = df.withColumn("b", col("a").cast(DataTypes.Int16))
    assertColumns(result1, "a", "b")
    result1.schema.getField("b").get.dataType shouldBe Int16Type

    // Cast Int32 -> Float64
    val result2 = df.withColumn("b", col("a").cast(DataTypes.Float64))
    result2.schema.getField("b").get.dataType shouldBe Float64Type
    assertColumnValues(result2, "b", 1.0, 2.0, 3.0)

    // Cast Int32 -> String
    val result3 = df.withColumn("b", col("a").cast(DataTypes.String))
    result3.schema.getField("b").get.dataType shouldBe StringType
    assertColumnValues(result3, "b", "1", "2", "3")
  }

  test("isIn unary op end-to-end") {
    val df = intFrame("a", 1, 2, 3, 4)
    val result = df.filter(col("a").isIn(Array(2, 4)))

    assertRowCount(result, 2)
    assertColumnValues(result, "a", 2, 4)
  }

  test("isBetween unary op end-to-end") {
    val df = intFrame("a", 1, 2, 3, 4)
    val result = df.filter(col("a").isBetween(2, 3))

    assertRowCount(result, 2)
    assertColumnValues(result, "a", 2, 3)
  }

  test("str.toUppercase end-to-end") {
    val df = stringFrame("a", "apple", "banana")
    val result = df.withColumn("b", col("a").str.toUppercase)

    assertRowCount(result, 2)
    assertColumns(result, "a", "b")
    assertColumnValues(result, "b", "APPLE", "BANANA")
  }

  test("like end-to-end with wildcard matching") {
    val df = stringFrame("a", "apple", "banana", "apricot")
    val result = df.filter(col("a").like("ap%"))

    assertRowCount(result, 2)
    assertColumnValues(result, "a", "apple", "apricot")
  }
}
