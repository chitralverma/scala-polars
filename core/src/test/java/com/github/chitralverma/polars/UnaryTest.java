package com.github.chitralverma.polars;

import static com.github.chitralverma.polars.functions.*;

import com.github.chitralverma.polars.api.DataFrame;
import com.github.chitralverma.polars.testing.AbstractPolarsJavaTest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Java mirror of {@code UnarySuite}. Replicates and asserts behaviours tested in upstream py-polars
 * for core unary transforms and predicates in Java.
 */
public class UnaryTest extends AbstractPolarsJavaTest {

  @Test
  public void unaryPredicates() {
    DataFrame df = doubleFrame("ColX", 1.0, Double.NaN, Double.POSITIVE_INFINITY);

    // Test isFinite
    DataFrame dfFinite = df.select(col("ColX").isFinite().alias("finite"));
    assertColumnValues(dfFinite, "finite", true, false, false);

    // Test isInfinite
    DataFrame dfInfinite = df.select(col("ColX").isInfinite().alias("infinite"));
    assertColumnValues(dfInfinite, "infinite", false, false, true);

    // Test dropNans
    DataFrame dfDropNans = df.select(col("ColX").dropNans().alias("non_nan"));
    assertColumnValues(dfDropNans, "non_nan", 1.0, Double.POSITIVE_INFINITY);

    // Test dropNulls using shift-introduced nulls
    DataFrame dfShifted = intFrame("ColY", 1, 2, 3);
    DataFrame dfDropNulls = dfShifted.select(col("ColY").shift(1).dropNulls().alias("non_null"));
    assertColumnValues(dfDropNulls, "non_null", 1, 2);

    // Test isEmpty
    DataFrame dfEmpty = df.select(col("ColX").isEmpty().alias("empty"));
    assertColumnValues(dfEmpty, "empty", false);
  }

  @Test
  public void unaryTransforms() {
    DataFrame df = intFrame("ColX", 1, 2, 3, 4, 5);

    // Test reverse
    DataFrame dfReverse = df.select(col("ColX").reverse().alias("reversed"));
    assertColumnValues(dfReverse, "reversed", 5, 4, 3, 2, 1);

    // Test slice
    DataFrame dfSlice = df.select(col("ColX").slice(1, 3).alias("sliced"));
    assertColumnValues(dfSlice, "sliced", 2, 3, 4);

    // Test head / limit
    DataFrame dfHead = df.select(col("ColX").head(2).alias("head"));
    assertColumnValues(dfHead, "head", 1, 2);

    DataFrame dfLimit = df.select(col("ColX").limit(2).alias("limit"));
    assertColumnValues(dfLimit, "limit", 1, 2);

    // Test tail
    DataFrame dfTail = df.select(col("ColX").tail(2).alias("tail"));
    assertColumnValues(dfTail, "tail", 4, 5);

    // Test gatherEvery
    DataFrame dfGather = df.select(col("ColX").gatherEvery(2, 1).alias("gathered"));
    assertColumnValues(dfGather, "gathered", 2, 4);

    // Test shift (positive and negative)
    DataFrame dfShift = df.select(col("ColX").shift(2).alias("shifted"));
    assertColumnValues(dfShift, "shifted", null, null, 1, 2, 3);

    DataFrame dfShiftNeg = df.select(col("ColX").shift(-2).alias("shifted_neg"));
    assertColumnValues(dfShiftNeg, "shifted_neg", 3, 4, 5, null, null);

    // Test gatherEvery argument validation
    try {
      col("ColX").gatherEvery(0);
      Assert.fail("Expected IllegalArgumentException for n == 0");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("must be >= 1"));
    }

    try {
      col("ColX").gatherEvery(2, -1);
      Assert.fail("Expected IllegalArgumentException for offset < 0");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("must be >= 0"));
    }
  }

  @Test
  public void unaryDistincts() {
    DataFrame df = intFrame("ColX", 1, 2, 2, 3, 1);

    // Test unique (without maintaining order)
    DataFrame dfUnique = df.select(col("ColX").unique(false).alias("unique"));
    List<Object> uniqueList = columnOf(dfUnique, "unique");
    Collections.sort((List) uniqueList);
    Assert.assertEquals(Arrays.asList(1, 2, 3), uniqueList);

    // Test unique (maintaining order)
    DataFrame dfUniqueStable = df.select(col("ColX").unique(true).alias("unique"));
    assertColumnValues(dfUniqueStable, "unique", 1, 2, 3);

    // Test isUnique
    DataFrame dfIsUnique = df.select(col("ColX").isUnique().alias("is_unique"));
    assertColumnValues(dfIsUnique, "is_unique", false, false, false, true, false);

    // Test isDuplicated
    DataFrame dfIsDuplicated = df.select(col("ColX").isDuplicated().alias("is_duplicated"));
    assertColumnValues(dfIsDuplicated, "is_duplicated", true, true, true, false, true);

    // Test isFirstDistinct
    DataFrame dfIsFirst = df.select(col("ColX").isFirstDistinct().alias("is_first"));
    assertColumnValues(dfIsFirst, "is_first", true, true, false, true, false);

    // Test isLastDistinct
    DataFrame dfIsLast = df.select(col("ColX").isLastDistinct().alias("is_last"));
    assertColumnValues(dfIsLast, "is_last", false, false, true, true, true);
  }

  @Test
  public void unaryDistinctAggregations() {
    DataFrame df = intFrame("ColX", 2, 2, 3, 1, 2);

    // Test mode
    DataFrame dfMode = df.select(col("ColX").mode().alias("mode"));
    assertColumnValues(dfMode, "mode", 2);

    // Test uniqueCounts
    DataFrame dfUniqueCounts = df.select(col("ColX").uniqueCounts().alias("counts"));
    assertColumnValues(dfUniqueCounts, "counts", 3, 1, 1);
  }

  @Test
  public void sortingAndTopKWithDuplicateExpressions() {
    DataFrame df = intFrame("ColX", 3, 1, 2);

    // Test sort with duplicate expressions and matching null_last array length
    DataFrame sortedDf =
        df.toLazy()
            .sort(
                new com.github.chitralverma.polars.api.expressions.Expression[] {
                  col("ColX"), col("ColX")
                },
                new boolean[] {true, true},
                false)
            .collect();
    assertColumnValues(sortedDf, "ColX", 1, 2, 3); // ascending twice (default)

    // Test topK with duplicate expressions and matching nullLast array length
    DataFrame topKDf =
        df.toLazy()
            .topK(
                2,
                new com.github.chitralverma.polars.api.expressions.Expression[] {
                  col("ColX"), col("ColX")
                },
                new boolean[] {true, true},
                false)
            .collect();
    assertColumnValues(topKDf, "ColX", 3, 2);
  }
}
