package com.github.chitralverma.polars

import com.github.chitralverma.polars.api.expressions.Expression
import com.github.chitralverma.polars.functions._
import com.github.chitralverma.polars.testing.PolarsTestBase

/** Replicates and asserts behaviours tested in upstream py-polars for core unary transforms and
  * predicates, such as null/nan filters, positional slices, and unique/distinct masks.
  */
class UnarySuite extends PolarsTestBase {

  test("unary predicates: isFinite, isInfinite, isEmpty, dropNulls, dropNans") {
    val df = doubleFrame("ColX", 1.0, Double.NaN, Double.PositiveInfinity)

    // Test isFinite
    val dfFinite = df.select(col("ColX").isFinite.alias("finite"))
    assertColumnValues(dfFinite, "finite", true, false, false)

    // Test isInfinite
    val dfInfinite = df.select(col("ColX").isInfinite.alias("infinite"))
    assertColumnValues(dfInfinite, "infinite", false, false, true)

    // Test dropNans
    val dfDropNans = df.select(col("ColX").dropNans().alias("non_nan"))
    assertColumnValues(dfDropNans, "non_nan", 1.0, Double.PositiveInfinity)

    // Test dropNulls using shift-introduced nulls
    val dfShifted = intFrame("ColY", 1, 2, 3)
    val dfDropNulls = dfShifted.select(col("ColY").shift(1).dropNulls().alias("non_null"))
    assertColumnValues(dfDropNulls, "non_null", 1, 2)

    // Test isEmpty
    val dfEmpty = df.select(col("ColX").isEmpty.alias("empty"))
    assertColumnValues(dfEmpty, "empty", false)
  }

  test("unary transforms: reverse, slice, head, tail, limit, gatherEvery, shift") {
    val df = intFrame("ColX", 1, 2, 3, 4, 5)

    // Test reverse
    val dfReverse = df.select(col("ColX").reverse().alias("reversed"))
    assertColumnValues(dfReverse, "reversed", 5, 4, 3, 2, 1)

    // Test slice
    val dfSlice = df.select(col("ColX").slice(1, 3).alias("sliced"))
    assertColumnValues(dfSlice, "sliced", 2, 3, 4)

    // Test head / limit
    val dfHead = df.select(col("ColX").head(2).alias("head"))
    assertColumnValues(dfHead, "head", 1, 2)

    val dfLimit = df.select(col("ColX").limit(2).alias("limit"))
    assertColumnValues(dfLimit, "limit", 1, 2)

    // Test tail
    val dfTail = df.select(col("ColX").tail(2).alias("tail"))
    assertColumnValues(dfTail, "tail", 4, 5)

    // Test gatherEvery
    val dfGather = df.select(col("ColX").gatherEvery(2, 1).alias("gathered"))
    assertColumnValues(dfGather, "gathered", 2, 4)

    // Test shift (positive and negative)
    val dfShift = df.select(col("ColX").shift(2).alias("shifted"))
    assertColumnValues(dfShift, "shifted", null, null, 1, 2, 3)

    val dfShiftNeg = df.select(col("ColX").shift(-2).alias("shifted_neg"))
    assertColumnValues(dfShiftNeg, "shifted_neg", 3, 4, 5, null, null)

    // Test gatherEvery argument validation
    an[IllegalArgumentException] shouldBe thrownBy {
      col("ColX").gatherEvery(0)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      col("ColX").gatherEvery(2, -1)
    }
  }

  test("unary distincts: unique, isUnique, isDuplicated, isFirstDistinct, isLastDistinct") {
    val df = intFrame("ColX", 1, 2, 2, 3, 1)

    // Test unique (without maintaining order)
    val dfUnique = df.select(col("ColX").unique(maintainOrder = false).alias("unique"))
    // unique returns [1, 2, 3] in some order (usually ascending or hash order)
    val uniqueVals = dfUnique.rows().map(_.getInt(0)).toSeq.sorted
    uniqueVals shouldBe Seq(1, 2, 3)

    // Test unique (maintaining order)
    val dfUniqueStable = df.select(col("ColX").unique(maintainOrder = true).alias("unique"))
    assertColumnValues(dfUniqueStable, "unique", 1, 2, 3)

    // Test isUnique (mask of elements that occur exactly once)
    val dfIsUnique = df.select(col("ColX").isUnique.alias("is_unique"))
    assertColumnValues(dfIsUnique, "is_unique", false, false, false, true, false)

    // Test isDuplicated (mask of elements that occur more than once)
    val dfIsDuplicated = df.select(col("ColX").isDuplicated.alias("is_duplicated"))
    assertColumnValues(dfIsDuplicated, "is_duplicated", true, true, true, false, true)

    // Test isFirstDistinct (mask of first occurrence of distinct elements)
    val dfIsFirst = df.select(col("ColX").isFirstDistinct.alias("is_first"))
    assertColumnValues(dfIsFirst, "is_first", true, true, false, true, false)

    // Test isLastDistinct (mask of last occurrence of distinct elements)
    val dfIsLast = df.select(col("ColX").isLastDistinct.alias("is_last"))
    assertColumnValues(dfIsLast, "is_last", false, false, true, true, true)
  }

  test("unary distinct aggregations: mode, uniqueCounts") {
    val df = intFrame("ColX", 2, 2, 3, 1, 2)

    // Test mode (most frequent values)
    val dfMode = df.select(col("ColX").mode().alias("mode"))
    assertColumnValues(dfMode, "mode", 2)

    // Test uniqueCounts (number of occurrences per unique value in order of appearance)
    val dfUniqueCounts = df.select(col("ColX").uniqueCounts().alias("counts"))
    assertColumnValues(dfUniqueCounts, "counts", 3, 1, 1)
  }

  test("sorting and top-K with duplicate expressions") {
    val df = intFrame("ColX", 3, 1, 2)

    // Test sort with duplicate expressions and matching null_last array length
    val sortedDf = df.toLazy
      .sort(Array[Expression](col("ColX"), col("ColX")), Array(true, true), maintainOrder = false)
      .collect()
    assertColumnValues(sortedDf, "ColX", 1, 2, 3) // ascending twice (default)

    // Test topK with duplicate expressions and matching nullLast array length
    val topKDf = df.toLazy
      .topK(
        2,
        Array[Expression](col("ColX"), col("ColX")),
        Array(true, true),
        maintainOrder = false
      )
      .collect()
    assertColumnValues(topKDf, "ColX", 3, 2)
  }

}
