package com.github.chitralverma.polars

import com.github.chitralverma.polars.functions._
import com.github.chitralverma.polars.testing.PolarsTestBase

/** Replicates and asserts behaviours tested in upstream py-polars for core expression-level and
  * horizontal aggregations.
  */
class AggregationSuite extends PolarsTestBase {

  test("expression-level statistical and value reductions") {
    val df = intFrame("ColX", 1, 2, 3, 4)

    // sum, min, max, mean, median
    assertColumnValues(df.select(col("ColX").sum().alias("sum")), "sum", 10)
    assertColumnValues(df.select(col("ColX").min().alias("min")), "min", 1)
    assertColumnValues(df.select(col("ColX").max().alias("max")), "max", 4)
    assertColumnValues(df.select(col("ColX").mean().alias("mean")), "mean", 2.5)
    assertColumnValues(df.select(col("ColX").median().alias("median")), "median", 2.5)

    // std, var (ddof default = 1)
    assertColumnValues(df.select(col("ColX").std().alias("std")), "std", 1.2909944487358056)
    assertColumnValues(df.select(col("ColX").`var`().alias("var")), "var", 1.6666666666666667)
    assertColumnValues(df.select(col("ColX").variance().alias("var")), "var", 1.6666666666666667)

    // product, count, len, nUnique, approxNUnique, nullCount
    assertColumnValues(df.select(col("ColX").product().alias("prod")), "prod", 24L)
    assertColumnValues(df.select(col("ColX").count().alias("count")), "count", 4)
    assertColumnValues(df.select(col("ColX").len().alias("len")), "len", 4)
    assertColumnValues(df.select(col("ColX").nUnique().alias("n_uniq")), "n_uniq", 4)
    assertColumnValues(df.select(col("ColX").approxNUnique().alias("approx")), "approx", 4)
    assertColumnValues(df.select(col("ColX").nullCount().alias("nulls")), "nulls", 0)

    // first, last
    assertColumnValues(df.select(col("ColX").first().alias("first")), "first", 1)
    assertColumnValues(df.select(col("ColX").last().alias("last")), "last", 4)

    // any, all (on boolean expression), cumSum
    val dfBool = df.select((col("ColX") > 2).alias("b")) // [false, false, true, true]
    assertColumnValues(dfBool.select(col("b").any().alias("any_b")), "any_b", true)
    assertColumnValues(dfBool.select(col("b").all().alias("all_b")), "all_b", false)

    val dfCum = df.select(col("ColX").cumSum().alias("cum_sum"))
    assertColumnValues(dfCum, "cum_sum", 1, 3, 6, 10)

    // Test ddof validation (Copilot check)
    an[IllegalArgumentException] shouldBe thrownBy {
      col("ColX").std(-1)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      col("ColX").`var`(256)
    }
  }

  test("expression-level indexing, sorting, shape, and quantile reductions") {
    val df = intFrame("ColX", 3, 1, 4, 2)

    // argMin, argMax
    assertColumnValues(df.select(col("ColX").argMin().alias("argmin")), "argmin", 1)
    assertColumnValues(df.select(col("ColX").argMax().alias("argmax")), "argmax", 2)

    // argSort
    val dfSort = df.select(col("ColX").argSort().alias("sort_idx"))
    assertColumnValues(dfSort, "sort_idx", 1, 3, 0, 2) // sorts to indices [1, 3, 0, 2]

    // skew, kurtosis
    assertColumnValues(df.select(col("ColX").skew().alias("skew")), "skew", 0.0)
    assertColumnValues(df.select(col("ColX").kurtosis().alias("kurt")), "kurt", -1.36)

    // quantile (Nearest/Linear)
    assertColumnValues(df.select(col("ColX").quantile(0.5, "nearest").alias("q")), "q", 3.0)
    assertColumnValues(df.select(col("ColX").quantile(0.5, "linear").alias("q")), "q", 2.5)
  }

  test("free companion functions mirroring pl.*") {
    val df = intFrame("ColX", 1, 2, 3, 4)

    // sum, min, max, mean, median, std, var, count, nUnique, approxNUnique, first, last, quantile, any, all, cumSum
    assertColumnValues(df.select(sum("ColX").alias("sum")), "sum", 10)
    assertColumnValues(df.select(min("ColX").alias("min")), "min", 1)
    assertColumnValues(df.select(max("ColX").alias("max")), "max", 4)
    assertColumnValues(df.select(mean("ColX").alias("mean")), "mean", 2.5)
    assertColumnValues(df.select(median("ColX").alias("median")), "median", 2.5)
    assertColumnValues(df.select(std("ColX").alias("std")), "std", 1.2909944487358056)
    assertColumnValues(df.select(variance("ColX").alias("var")), "var", 1.6666666666666667)
    assertColumnValues(df.select(count("ColX").alias("count")), "count", 4)
    assertColumnValues(df.select(nUnique("ColX").alias("n_uniq")), "n_uniq", 4)
    assertColumnValues(df.select(approxNUnique("ColX").alias("approx")), "approx", 4)
    assertColumnValues(df.select(first("ColX").alias("first")), "first", 1)
    assertColumnValues(df.select(last("ColX").alias("last")), "last", 4)
    assertColumnValues(df.select(quantile("ColX", 0.5, "linear").alias("q")), "q", 2.5)

    val dfBool = df.select((col("ColX") > 2).alias("b"))
    assertColumnValues(dfBool.select(any("b").alias("any_b")), "any_b", true)
    assertColumnValues(dfBool.select(functions.all("b").alias("all_b")), "all_b", false)

    assertColumnValues(df.select(cumSum("ColX").alias("cum_sum")), "cum_sum", 1, 3, 6, 10)

    // len() free function row-count helper
    assertColumnValues(df.select(len().alias("total_rows")), "total_rows", 4)
  }

  test(
    "horizontal aggregates: sumHorizontal, meanHorizontal, minHorizontal, maxHorizontal, allHorizontal, anyHorizontal"
  ) {
    val df = intFrame("A", 1, 2, 3).withColumn("B", col("A") * 10) // B = [10, 20, 30]

    // sumHorizontal, meanHorizontal
    val dfSum = df.select(sumHorizontal(col("A"), col("B")).alias("sum_h"))
    assertColumnValues(dfSum, "sum_h", 11, 22, 33)

    val dfMean = df.select(meanHorizontal(col("A"), col("B")).alias("mean_h"))
    assertColumnValues(dfMean, "mean_h", 5.5, 11.0, 16.5)

    // minHorizontal, maxHorizontal
    val dfMin = df.select(minHorizontal(col("A"), col("B")).alias("min_h"))
    assertColumnValues(dfMin, "min_h", 1, 2, 3)

    val dfMax = df.select(maxHorizontal(col("A"), col("B")).alias("max_h"))
    assertColumnValues(dfMax, "max_h", 10, 20, 30)

    // boolean horizontals
    val dfBase = intFrame("ColX", 2, 2, 3, 1)
    val dfBool = dfBase
      .withColumn("A_bool", col("ColX") === 2) // [true, true, false, false]
      .withColumn("B_bool", col("ColX") >= 2) // [true, true, true, false]

    val dfAny = dfBool.select(anyHorizontal(col("A_bool"), col("B_bool")).alias("any_h"))
    assertColumnValues(dfAny, "any_h", true, true, true, false)

    val dfAll = dfBool.select(allHorizontal(col("A_bool"), col("B_bool")).alias("all_h"))
    assertColumnValues(dfAll, "all_h", true, true, false, false)
  }

  test("test_vertical: alias for col agg (equivalence of free-fn and expr form)") {
    // Replicates test_vertical.py::test_alias_for_col_agg and test_alias_for_col_agg_bool
    // Skipped: selectors (cs.*), as_expression (not yet supported)
    val df = intFrame("a", 1, 4)
    assertColumnValues(df.select(min("a").alias("m")), "m", 1)
    assertColumnValues(df.select(max("a").alias("m")), "m", 4)
    assertColumnValues(df.select(sum("a").alias("m")), "m", 5)
    assertColumnValues(df.select(cumSum("a").alias("m")), "m", 1, 5)

    val dfBool = booleanFrame("b", true, false)
    assertColumnValues(dfBool.select(any("b").alias("any")), "any", true)
    assertColumnValues(dfBool.select(functions.all("b").alias("all")), "all", false)
  }

  test("test_horizontal: max_min_nulls_consistency, nested_min_max, broadcasting, and raises") {
    // Replicates test_horizontal.py::test_max_min_nulls_consistency (using shift for nulls)
    // Skipped: cum_sum_horizontal (deferred to Phase 10 Structs), pl.duration, Decimal dtypes
    val dfNulls = intFrame("a", 10, 20, 30)
      .withColumn("b", col("a").shift(1)) // [null, 10, 20]
      .withColumn("c", col("a").shift(-1)) // [20, 30, null]
    assertColumnValues(
      dfNulls.select(maxHorizontal(col("a"), col("b"), col("c")).alias("max_h")),
      "max_h",
      20,
      30,
      30
    )
    assertColumnValues(
      dfNulls.select(minHorizontal(col("a"), col("b"), col("c")).alias("min_h")),
      "min_h",
      10,
      10,
      20
    )

    // Replicates test_horizontal.py::test_nested_min_max
    val dfNested =
      intFrame("a", 1).withColumn("b", lit(2)).withColumn("c", lit(3)).withColumn("d", lit(4))
    val dfNestedRes = dfNested.select(
      maxHorizontal(minHorizontal(col("a"), col("b")), minHorizontal(col("c"), col("d")))
        .alias("t")
    )
    assertColumnValues(dfNestedRes, "t", 3)

    // Replicates test_horizontal.py::test_horizontal_broadcasting
    val dfBroad = intFrame("a", 1, 3).withColumn("b", lit(3)) // b = [3, 3] (literal broadcasted)
    assertColumnValues(
      dfBroad.select(sumHorizontal(lit(1), col("a"), col("b")).alias("sum_h")),
      "sum_h",
      5,
      7
    )

    // Replicates test_horizontal.py::test_mean_horizontal_bool
    val dfBoolMean = booleanFrame("a", true, false, false)
      .withColumn("b", col("a").reverse()) // [false, false, true]
    assertColumnValues(
      dfBoolMean.select(meanHorizontal(col("a"), col("b")).alias("mean_h")),
      "mean_h",
      0.5,
      0.0,
      0.5
    )

    // Replicates test_horizontal.py::test_raise_invalid_types_21835 (min_horizontal on int and string raises)
    val dfInvalid = api.DataFrame.fromSeries(
      api.Series.ofInt("x", Array(1)),
      api.Series.ofString("y", Array("two"))
    )
    assertThrows[RuntimeException] {
      dfInvalid.select(minHorizontal(col("x"), col("y")))
    }
  }

}
