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

    // Test is_finite
    DataFrame dfFinite = df.select(col("ColX").is_finite().alias("finite"));
    assertColumnValues(dfFinite, "finite", true, false, false);

    // Test is_infinite
    DataFrame dfInfinite = df.select(col("ColX").is_infinite().alias("infinite"));
    assertColumnValues(dfInfinite, "infinite", false, false, true);

    // Test drop_nans
    DataFrame dfDropNans = df.select(col("ColX").drop_nans().alias("non_nan"));
    assertColumnValues(dfDropNans, "non_nan", 1.0, Double.POSITIVE_INFINITY);

    // Test drop_nulls using shift-introduced nulls
    DataFrame dfShifted = intFrame("ColY", 1, 2, 3);
    DataFrame dfDropNulls = dfShifted.select(col("ColY").shift(1).drop_nulls().alias("non_null"));
    assertColumnValues(dfDropNulls, "non_null", 1, 2);

    // Test is_empty
    DataFrame dfEmpty = df.select(col("ColX").is_empty().alias("empty"));
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

    // Test gather_every
    DataFrame dfGather = df.select(col("ColX").gather_every(2, 1).alias("gathered"));
    assertColumnValues(dfGather, "gathered", 2, 4);

    // Test shift (positive and negative)
    DataFrame dfShift = df.select(col("ColX").shift(2).alias("shifted"));
    assertColumnValues(dfShift, "shifted", null, null, 1, 2, 3);

    DataFrame dfShiftNeg = df.select(col("ColX").shift(-2).alias("shifted_neg"));
    assertColumnValues(dfShiftNeg, "shifted_neg", 3, 4, 5, null, null);
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

    // Test is_unique
    DataFrame dfIsUnique = df.select(col("ColX").is_unique().alias("is_unique"));
    assertColumnValues(dfIsUnique, "is_unique", false, false, false, true, false);

    // Test is_duplicated
    DataFrame dfIsDuplicated = df.select(col("ColX").is_duplicated().alias("is_duplicated"));
    assertColumnValues(dfIsDuplicated, "is_duplicated", true, true, true, false, true);

    // Test is_first_distinct
    DataFrame dfIsFirst = df.select(col("ColX").is_first_distinct().alias("is_first"));
    assertColumnValues(dfIsFirst, "is_first", true, true, false, true, false);

    // Test is_last_distinct
    DataFrame dfIsLast = df.select(col("ColX").is_last_distinct().alias("is_last"));
    assertColumnValues(dfIsLast, "is_last", false, false, true, true, true);
  }

  @Test
  public void unaryDistinctAggregations() {
    DataFrame df = intFrame("ColX", 2, 2, 3, 1, 2);

    // Test mode
    DataFrame dfMode = df.select(col("ColX").mode().alias("mode"));
    assertColumnValues(dfMode, "mode", 2);

    // Test unique_counts
    DataFrame dfUniqueCounts = df.select(col("ColX").unique_counts().alias("counts"));
    assertColumnValues(dfUniqueCounts, "counts", 3, 1, 1);
  }
}
