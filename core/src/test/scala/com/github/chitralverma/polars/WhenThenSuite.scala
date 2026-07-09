package com.github.chitralverma.polars

import com.github.chitralverma.polars.api.{DataFrame, Series}
import com.github.chitralverma.polars.functions._
import com.github.chitralverma.polars.testing.PolarsTestBase

/** Tests the `when / otherwise` conditional builder. */
class WhenThenSuite extends PolarsTestBase {

  test("when/otherwise: basic branch and implicit null default") {
    val df = intFrame("a", 1, 2, 3, 4, 5)

    // With otherwise -> unmatched rows take the default.
    val withOtherwise = df.select(
      when(col("a") < 3, "x").otherwise("y").alias("a")
    )
    assertColumnValues(withOtherwise, "a", "x", "x", "y", "y", "y")

    // Without otherwise -> unmatched rows are null (implicit otherwise(null)).
    val implicitNull = df.select(when(col("a") < 3, "x").alias("b"))
    assertColumnValues(implicitNull, "b", "x", "x", null, null, null)
  }

  test("when/otherwise: chained conditions") {
    val df = intFrame("a", 1, 2, 3, 4, 5)

    val withOtherwise = df.select(
      when(col("a") < 3, "x")
        .when(col("a") > 4, "z")
        .otherwise("y")
        .alias("a")
    )
    assertColumnValues(withOtherwise, "a", "x", "x", "y", "y", "z")

    // Chained builder used without otherwise -> unmatched rows are null.
    val implicitNull = df.select(
      when(col("a") < 3, "x")
        .when(col("a") > 4, "z")
        .alias("b")
    )
    assertColumnValues(implicitNull, "b", "x", "x", null, null, "z")
  }

  test("when: bare when usable as an expression (implicit none)") {
    val df = DataFrame.fromSeries(
      Series.ofString("team", Array("A", "A", "A", "B", "B", "C")),
      Series.ofInt("points", Array(11, 8, 10, 6, 6, 5))
    )

    val result = df.select(
      when(col("points") > 7, "Foo").alias("flag")
    )
    assertColumnValues(result, "flag", "Foo", "Foo", "Foo", null, null, null)
  }

  test("when/otherwise: value taken from another column") {
    val df = DataFrame.fromSeries(
      Series.ofInt("x", Array(0, 1, 2, 3, 4)),
      Series.ofInt("y", Array(5, 6, 7, 8, 9))
    )

    // A column expression is used directly as the branch value.
    val result = df.select(
      when(col("x") < 2, col("x")).otherwise(col("y")).alias("out")
    )
    assertColumnValues(result, "out", 0, 1, 7, 8, 9)
  }

  test("when/otherwise: string supertype coercion") {
    // A numeric then-branch and a string otherwise-branch are coerced to a common string supertype.
    val df = DataFrame.fromSeries(
      Series.ofString("names", Array("foo", "spam", "spam")),
      Series.ofInt("nrs", Array(1, 2, 3))
    )

    val result = df.select(
      when(col("names") === "spam", col("nrs") * 2)
        .otherwise(lit("other"))
        .alias("new_col")
    )
    assertColumnValues(result, "new_col", "other", "4", "6")
  }

  test("when/otherwise: numeric supertype cascade") {
    // Mixed Int/Double branch values are coerced to a common floating supertype.
    val df = intFrame("foo", 1, 3, 4)

    val result = df.select(
      when(col("foo") === 1, 1)
        .when(col("foo") === 2, 4)
        .when(col("foo") === 3, 1.5)
        .when(col("foo") === 4, 16)
        .otherwise(0)
        .alias("val")
    )
    assertColumnValues(result, "val", 1.0, 1.5, 16.0)
  }

  test("when/otherwise: long chain folds correctly") {
    // A long chain frees several intermediate handles during the fold; the result must stay valid.
    val df = intFrame("n", 1, 2, 3, 4, 5, 6, 7)

    val result = df.select(
      when(col("n") === 1, "one")
        .when(col("n") === 2, "two")
        .when(col("n") === 3, "three")
        .when(col("n") === 4, "four")
        .when(col("n") === 5, "five")
        .when(col("n") === 6, "six")
        .otherwise("many")
        .alias("word")
    )
    assertColumnValues(result, "word", "one", "two", "three", "four", "five", "six", "many")
  }

  test("when/otherwise: nested via any_horizontal equals chained") {
    val df = DataFrame.fromSeries(
      Series.ofString("c1", Array("a", "b")),
      Series.ofString("c2", Array("c", "d"))
    )

    // Nested otherwise(when(...)) form.
    val nested = df.select(
      when(anyHorizontal(col("c1") === "a", col("c2") === "a"), "a")
        .otherwise(
          when(anyHorizontal(col("c1") === "d", col("c2") === "d"), "d")
            .otherwise(lit(null))
        )
        .alias("result")
    )

    // Flat chained form.
    val chained = df.select(
      when(anyHorizontal(col("c1") === "a", col("c2") === "a"), "a")
        .when(anyHorizontal(col("c1") === "d", col("c2") === "d"), "d")
        .otherwise(lit(null))
        .alias("result")
    )

    assertColumnValues(nested, "result", "a", "d")
    assertColumnValues(chained, "result", "a", "d")
  }

}
