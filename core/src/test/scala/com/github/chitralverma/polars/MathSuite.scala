package com.github.chitralverma.polars

import com.github.chitralverma.polars.api.types._
import com.github.chitralverma.polars.functions._
import com.github.chitralverma.polars.testing.PolarsTestBase

/** Tests arithmetic, rounding, logarithmic, and trigonometric expression transforms, covering
  * null propagation, special float values (NaN and infinity), and the input types the operations
  * reject.
  */
class MathSuite extends PolarsTestBase {

  private val tol = 1e-6

  private def valuesOf(df: api.DataFrame, name: String): Seq[AnyRef] = columnOf(df, name)

  private def assertApprox(df: api.DataFrame, name: String, expected: Any*): Unit = {
    val actual = valuesOf(df, name)
    actual.length shouldBe expected.length
    actual.zip(expected).foreach {
      case (a, null) => a shouldBe null
      case (a, e: Double) =>
        val d = a.asInstanceOf[Double]
        if (e.isNaN) d.isNaN shouldBe true
        else if (e.isInfinite) d shouldBe e
        else d shouldBe (e +- tol)
      case (a, e) => a shouldBe e
    }
  }

  test("negation preserves nulls and matches the operator form") {
    val df = intFrame("a", -1, 0, 1, 5).withColumn("a", col("a").shift(1)) // [null, -1, 0, 1]

    assertApprox(df.select(col("a").neg().alias("r")), "r", null, 1, 0, -1)
    assertApprox(df.select((-col("a")).alias("r")), "r", null, 1, 0, -1)
  }

  test("negation rejects unsigned and non-numeric inputs") {
    val dfU = intFrame("a", 1, 2, 3)
    assertThrows[RuntimeException](dfU.select(col("a").cast(UInt8Type).neg()).count())

    val dfS = stringFrame("a", "p", "q", "r")
    assertThrows[RuntimeException](dfS.select(col("a").neg()).count())
  }

  test("power raises values to a scalar exponent") {
    val df = doubleFrame("x", 2.0, 3.0, 4.0)
    assertApprox(df.select(col("x").pow(2.0).alias("r")), "r", 4.0, 9.0, 16.0)
    assertApprox(df.select(col("x").pow(0.0).alias("r")), "r", 1.0, 1.0, 1.0)
  }

  test("floor division rounds the quotient toward negative infinity") {
    val df = intFrame("a", 1, 2, 3).withColumn("d", lit(2))
    assertApprox(df.select(col("a").floorDiv(col("d")).alias("r")), "r", 0, 1, 1)
  }

  test("absolute value preserves nulls and rejects non-numeric input") {
    val df = intFrame("a", -1, 0, 1, 5).withColumn("a", col("a").shift(1)) // [null, -1, 0, 1]
    assertApprox(df.select(col("a").abs().alias("r")), "r", null, 1, 0, 1)

    // Operator and method forms agree.
    val dfF = doubleFrame("x", 1.0, -2.0, 3.0, -4.0)
    assertApprox(dfF.select(col("x").abs().alias("r")), "r", 1.0, 2.0, 3.0, 4.0)

    val dfS = stringFrame("a", "p", "q", "r")
    assertThrows[RuntimeException](dfS.select(col("a").abs()).count())
  }

  test("absolute value of a signed integer minimum wraps") {
    // The two's-complement minimum has no positive counterpart and wraps back to itself.
    val df = intFrame("a", -128).withColumn("a", col("a").cast(Int8Type))
    assertApprox(df.select(col("a").abs().alias("r")), "r", (-128).toByte)
  }

  test("floor and ceil round toward and away from zero") {
    val dfFloor = doubleFrame("x", 1.4, 1.5, 2.5, -1.6)
    assertApprox(dfFloor.select(col("x").floor().alias("r")), "r", 1.0, 1.0, 2.0, -2.0)

    val dfCeil = doubleFrame("x", 1.8, 1.2, 3.0)
    assertApprox(dfCeil.select(col("x").ceil().alias("r")), "r", 2.0, 2.0, 3.0)
  }

  test("round uses banker's rounding and is identity on integers") {
    val df = doubleFrame("x", 1.003, 2.003)
    assertApprox(df.select(col("x").round(2).alias("r")), "r", 1.0, 2.0)
    assertApprox(df.select(col("x").round().alias("r")), "r", 1.0, 2.0)

    // Round-half-to-even: 1.5 and 2.5 both round to 2.
    val dfHalf = doubleFrame("x", 0.5, 1.5, 2.5, 3.5)
    assertApprox(dfHalf.select(col("x").round().alias("r")), "r", 0.0, 2.0, 2.0, 4.0)

    // Rounding an integer column leaves it unchanged.
    val dfInt = intFrame("a", 1, 2, 3)
    assertApprox(dfInt.select(col("a").round().alias("r")), "r", 1, 2, 3)
  }

  test("round to significant figures works for floats and integers") {
    val dfF = doubleFrame("x", 1.234, 0.1234)
    assertApprox(dfF.select(col("x").roundSigFigs(2).alias("r")), "r", 1.2, 0.12)

    val dfI = intFrame("a", 123400, 1234)
    assertApprox(dfI.select(col("a").roundSigFigs(2).alias("r")), "r", 120000, 1200)

    val dfZero = doubleFrame("x", 0.0)
    assertApprox(dfZero.select(col("x").roundSigFigs(2).alias("r")), "r", 0.0)
  }

  test("truncate drops the fractional part toward zero across special values") {
    val df = doubleFrame("x", 1.003, 2.003)
    assertApprox(df.select(col("x").truncate(2).alias("r")), "r", 1.0, 2.0)
    assertApprox(df.select(col("x").truncate().alias("r")), "r", 1.0, 2.0)

    // Truncation of negatives rounds toward zero.
    val dfNeg = doubleFrame("x", -1.78, -2.56, -3.99)
    assertApprox(dfNeg.select(col("x").truncate(1).alias("r")), "r", -1.7, -2.5, -3.9)

    // Integers are unchanged.
    val dfInt = intFrame("a", 1, 2, 3)
    assertApprox(dfInt.select(col("a").truncate().alias("r")), "r", 1, 2, 3)

    // NaN, infinity, and null flow through unchanged.
    val dfSpecial = doubleFrame("x", 1.5, Double.NaN, Double.PositiveInfinity)
    assertApprox(
      dfSpecial.select(col("x").truncate(1).alias("r")),
      "r",
      1.5,
      Double.NaN,
      Double.PositiveInfinity
    )
  }

  test("sign returns -1, 0, or 1 and preserves nulls and NaN") {
    val dfInt = intFrame("a", -9, 0, 4, 7).withColumn("a", col("a").shift(1)) // [null, -9, 0, 4]
    assertApprox(dfInt.select(col("a").sign().alias("r")), "r", null, -1, 0, 1)

    val dfFloat = doubleFrame("x", -9.0, 0.0, 4.0, Double.NaN)
    assertApprox(dfFloat.select(col("x").sign().alias("r")), "r", -1.0, 0.0, 1.0, Double.NaN)

    // Sign of a date column is not defined.
    val dfDate = intFrame("a", 1).withColumn("a", col("a").cast(DateType))
    assertThrows[RuntimeException](dfDate.select(col("a").sign()).count())
  }

  test("clip limits values to a range and each single bound") {
    val df = intFrame("a", 1, 2, 3, 4, 5).withColumn("mn", lit(0)).withColumn("mx", lit(2))

    assertApprox(df.select(col("a").clip(col("mn"), col("mx")).alias("r")), "r", 1, 2, 2, 2, 2)
    assertApprox(df.select(col("a").clipMin(col("mn")).alias("r")), "r", 1, 2, 3, 4, 5)
    assertApprox(df.select(col("a").clipMax(col("mx")).alias("r")), "r", 1, 2, 2, 2, 2)
  }

  test("clip rejects non-numeric input") {
    val dfS = stringFrame("a", "a", "b", "c")
    assertThrows[RuntimeException](dfS.select(col("a").clip(lit("b"), lit("z"))).count())
  }

  test("square and cube roots") {
    val dfSqrt = doubleFrame("x", 1.0, 2.0, 4.0)
    assertApprox(dfSqrt.select(col("x").sqrt().alias("r")), "r", 1.0, math.sqrt(2.0), 2.0)

    val dfCbrt = doubleFrame("x", 1.0, 2.0, 8.0)
    assertApprox(dfCbrt.select(col("x").cbrt().alias("r")), "r", 1.0, math.cbrt(2.0), 2.0)
  }

  test("exponential preserves nulls and runs on an empty column") {
    val df =
      doubleFrame("x", 0.1, 0.01, 99.0).withColumn("x", col("x").shift(1)) // [null, 0.1, 0.01]
    assertApprox(
      df.select(col("x").exp().alias("r")),
      "r",
      null,
      1.1051709180756477,
      1.010050167084168
    )

    val dfEmpty = doubleFrame("x").select(col("x").exp().alias("r"))
    valuesOf(dfEmpty, "r") shouldBe empty
  }

  test("logarithms with an arbitrary base and the base-e, base-10, and log1p forms") {
    val df = doubleFrame("x", 1.0, 3.0, 9.0, 27.0, 81.0)
    assertApprox(df.select(col("x").log(3.0).alias("r")), "r", 0.0, 1.0, 2.0, 3.0, 4.0)

    val dfE = doubleFrame("x", 1.0, math.E)
    assertApprox(dfE.select(col("x").log().alias("r")), "r", 0.0, 1.0)

    val dfTen = doubleFrame("x", 1.0, 10.0, 1000.0)
    assertApprox(dfTen.select(col("x").log10().alias("r")), "r", 0.0, 1.0, 3.0)

    val dfLog1p = doubleFrame("x", 0.0, math.E - 1.0)
    assertApprox(dfLog1p.select(col("x").log1p().alias("r")), "r", 0.0, 1.0)
  }

  test("difference between consecutive elements with lag and null handling") {
    val df = intFrame("a", 10, 20, 30, 40)
    // The first row has no predecessor and becomes null.
    assertApprox(df.select(col("a").diff().alias("r")), "r", null, 10, 10, 10)
    assertApprox(df.select(col("a").diff(2).alias("r")), "r", null, null, 20, 20)
  }

  test("percentage change over a lag, handling nulls") {
    val dfPos = doubleFrame("x", 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0)
    assertApprox(
      dfPos.select(col("x").pctChange(2).alias("r")),
      "r",
      null,
      null,
      3.0,
      3.0,
      3.0,
      3.0,
      3.0
    )

    // A null in the middle produces null both at that row and the following comparison.
    val dfNull = doubleFrame("x", 10.0, 11.0, 12.0, 999.0, 12.0, 24.0)
      .withColumn("x", when(col("x") === 999.0, lit(null)).otherwise(col("x")))
    assertApprox(
      dfNull.select(col("x").pctChange().alias("r")),
      "r",
      null,
      0.1,
      0.090909,
      null,
      null,
      1.0
    )
  }

  test("trigonometric transforms in radians preserve nulls and NaN") {
    val df = doubleFrame("x", 0.0, math.Pi, Double.NaN, -1.0)
      .withColumn("x", col("x").shift(1)) // [null, 0.0, Pi, NaN]

    assertApprox(
      df.select(col("x").sin().alias("r")),
      "r",
      null,
      0.0,
      math.sin(math.Pi),
      Double.NaN
    )
    assertApprox(df.select(col("x").cos().alias("r")), "r", null, 1.0, -1.0, Double.NaN)
    assertApprox(
      df.select(col("x").tan().alias("r")),
      "r",
      null,
      0.0,
      math.tan(math.Pi),
      Double.NaN
    )

    // Cotangent is undefined at 0 (positive infinity) and preserves nulls and NaN.
    val cot = valuesOf(df.select(col("x").cot().alias("r")), "r")
    cot.head shouldBe null
    cot(1).asInstanceOf[Double] shouldBe Double.PositiveInfinity
    cot(3).asInstanceOf[Double].isNaN shouldBe true
  }

  test("inverse and hyperbolic trigonometric transforms") {
    val dfInv = doubleFrame("x", 0.0, 1.0)
    assertApprox(dfInv.select(col("x").arcsin().alias("r")), "r", 0.0, math.Pi / 2)
    assertApprox(dfInv.select(col("x").arccos().alias("r")), "r", math.Pi / 2, 0.0)
    assertApprox(dfInv.select(col("x").arctan().alias("r")), "r", 0.0, math.Pi / 4)

    val df = doubleFrame("x", 0.0, 1.0)
    assertApprox(df.select(col("x").sinh().alias("r")), "r", 0.0, math.sinh(1.0))
    assertApprox(df.select(col("x").cosh().alias("r")), "r", 1.0, math.cosh(1.0))
    assertApprox(df.select(col("x").tanh().alias("r")), "r", 0.0, math.tanh(1.0))
    assertApprox(df.select(col("x").arcsinh().alias("r")), "r", 0.0, 0.881373587019543)
    assertApprox(df.select(col("x").arctanh().alias("r")), "r", 0.0, Double.PositiveInfinity)

    val dfCosh = doubleFrame("x", 1.0, 2.0)
    assertApprox(dfCosh.select(col("x").arccosh().alias("r")), "r", 0.0, 1.3169578969248166)
  }

  test("trigonometric transforms reject non-numeric input") {
    val dfS = stringFrame("a", "1", "2", "3")
    assertThrows[RuntimeException](dfS.select(col("a").sin()).count())
  }

  test("degree and radian conversion") {
    val dfDeg = doubleFrame("x", 0.0, math.Pi)
    assertApprox(dfDeg.select(col("x").degrees().alias("r")), "r", 0.0, 180.0)

    val dfRad = doubleFrame("x", 0.0, 180.0)
    assertApprox(dfRad.select(col("x").radians().alias("r")), "r", 0.0, math.Pi)
  }

  test("two-argument arctangent chooses the correct quadrant") {
    val df = doubleFrame("y", 1.0, 1.0, -1.0).withColumn("x", lit(1.0))

    assertApprox(
      df.select(arctan2(col("y"), col("x")).alias("r")),
      "r",
      math.Pi / 4,
      math.Pi / 4,
      -math.Pi / 4
    )
    assertApprox(df.select(arctan2d(col("y"), col("x")).alias("r")), "r", 45.0, 45.0, -45.0)
    // The column-name overloads delegate to the expression forms.
    assertApprox(
      df.select(arctan2("y", "x").alias("r")),
      "r",
      math.Pi / 4,
      math.Pi / 4,
      -math.Pi / 4
    )
  }

  test("physical representation of an integer column is an identity") {
    val df = intFrame("a", 1, 2, 3)
    assertApprox(df.select(col("a").toPhysical().alias("r")), "r", 1, 2, 3)

    // A date column exposes its Int32 day-offset physical form.
    val dfDate = intFrame("a", 0, 1, 2).withColumn("a", col("a").cast(DateType))
    val physical = dfDate.select(col("a").toPhysical().alias("r"))
    physical.schema.getField("r").get.dataType shouldBe Int32Type
  }

  test("reinterpreting an unsigned integer as signed keeps the bit pattern") {
    val df = longFrame("a", 1L, 1L, 2L)
    val out = df.select(col("a").cast(UInt64Type).reinterpret(signed = true).alias("r"))
    assertApprox(out, "r", 1L, 1L, 2L)
    out.schema.getField("r").get.dataType shouldBe Int64Type
  }

  test("input validation for rounding parameters") {
    an[IllegalArgumentException] shouldBe thrownBy(col("a").round(-1))
    an[IllegalArgumentException] shouldBe thrownBy(col("a").roundSigFigs(0))
    an[IllegalArgumentException] shouldBe thrownBy(col("a").truncate(-1))
  }
}
