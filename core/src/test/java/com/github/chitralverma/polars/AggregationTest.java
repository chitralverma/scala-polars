package com.github.chitralverma.polars;

import static com.github.chitralverma.polars.functions.*;

import com.github.chitralverma.polars.api.DataFrame;
import com.github.chitralverma.polars.testing.AbstractPolarsJavaTest;
import org.junit.Test;

/**
 * Java mirror of {@code AggregationSuite}. Replicates and asserts behaviours tested in upstream
 * py-polars for core expression-level and horizontal aggregations in Java.
 */
public class AggregationTest extends AbstractPolarsJavaTest {

  @Test
  public void expressionLevelReductions() {
    DataFrame df = intFrame("ColX", 1, 2, 3, 4);

    // sum, min, max, mean, median
    assertColumnValues(df.select(col("ColX").sum().alias("sum")), "sum", 10);
    assertColumnValues(df.select(col("ColX").min().alias("min")), "min", 1);
    assertColumnValues(df.select(col("ColX").max().alias("max")), "max", 4);
    assertColumnValues(df.select(col("ColX").mean().alias("mean")), "mean", 2.5);
    assertColumnValues(df.select(col("ColX").median().alias("median")), "median", 2.5);

    // std, var (ddof default = 1)
    assertColumnValues(df.select(col("ColX").std().alias("std")), "std", 1.2909944487358056);
    assertColumnValues(df.select(col("ColX").var().alias("var")), "var", 1.6666666666666667);
    assertColumnValues(df.select(col("ColX").variance().alias("var")), "var", 1.6666666666666667);

    // product, count, len, nUnique, approxNUnique, nullCount
    assertColumnValues(df.select(col("ColX").product().alias("prod")), "prod", 24L);
    assertColumnValues(df.select(col("ColX").count().alias("count")), "count", 4);
    assertColumnValues(df.select(col("ColX").len().alias("len")), "len", 4);
    assertColumnValues(df.select(col("ColX").nUnique().alias("n_uniq")), "n_uniq", 4);
    assertColumnValues(df.select(col("ColX").approxNUnique().alias("approx")), "approx", 4);
    assertColumnValues(df.select(col("ColX").nullCount().alias("nulls")), "nulls", 0);

    // first, last
    assertColumnValues(df.select(col("ColX").first().alias("first")), "first", 1);
    assertColumnValues(df.select(col("ColX").last().alias("last")), "last", 4);
  }

  @Test
  public void expressionLevelSortingAndQuantiles() {
    DataFrame df = intFrame("ColX", 3, 1, 4, 2);

    // argMin, argMax
    assertColumnValues(df.select(col("ColX").argMin().alias("argmin")), "argmin", 1);
    assertColumnValues(df.select(col("ColX").argMax().alias("argmax")), "argmax", 2);

    // argSort
    DataFrame dfSort = df.select(col("ColX").argSort().alias("sort_idx"));
    assertColumnValues(dfSort, "sort_idx", 1, 3, 0, 2);

    // skew, kurtosis
    assertColumnValues(df.select(col("ColX").skew().alias("skew")), "skew", 0.0);
    assertColumnValues(df.select(col("ColX").kurtosis().alias("kurt")), "kurt", -1.36);

    // quantile (Nearest/Linear)
    assertColumnValues(df.select(col("ColX").quantile(0.5, "nearest").alias("q")), "q", 3.0);
    assertColumnValues(df.select(col("ColX").quantile(0.5, "linear").alias("q")), "q", 2.5);
  }

  @Test
  public void freeCompanionFunctions() {
    DataFrame df = intFrame("ColX", 1, 2, 3, 4);

    // sum, min, max, mean, median, std, var, count, nUnique, approxNUnique, first, last, quantile
    assertColumnValues(df.select(sum("ColX").alias("sum")), "sum", 10);
    assertColumnValues(df.select(min("ColX").alias("min")), "min", 1);
    assertColumnValues(df.select(max("ColX").alias("max")), "max", 4);
    assertColumnValues(df.select(mean("ColX").alias("mean")), "mean", 2.5);
    assertColumnValues(df.select(median("ColX").alias("median")), "median", 2.5);
    assertColumnValues(df.select(std("ColX").alias("std")), "std", 1.2909944487358056);
    assertColumnValues(df.select(variance("ColX").alias("var")), "var", 1.6666666666666667);
    assertColumnValues(df.select(count("ColX").alias("count")), "count", 4);
    assertColumnValues(df.select(nUnique("ColX").alias("n_uniq")), "n_uniq", 4);
    assertColumnValues(df.select(approxNUnique("ColX").alias("approx")), "approx", 4);
    assertColumnValues(df.select(first("ColX").alias("first")), "first", 1);
    assertColumnValues(df.select(last("ColX").alias("last")), "last", 4);
    assertColumnValues(df.select(quantile("ColX", 0.5, "linear").alias("q")), "q", 2.5);

    // len() free function
    assertColumnValues(df.select(len().alias("total_rows")), "total_rows", 4);
  }

  @Test
  public void horizontalAggregates() {
    DataFrame df = intFrame("A", 1, 2, 3).withColumn("B", col("A").multiply(10));

    // sumHorizontal, meanHorizontal
    DataFrame dfSum = df.select(sumHorizontal(col("A"), col("B")).alias("sum_h"));
    assertColumnValues(dfSum, "sum_h", 11, 22, 33);

    DataFrame dfMean = df.select(meanHorizontal(col("A"), col("B")).alias("mean_h"));
    assertColumnValues(dfMean, "mean_h", 5.5, 11.0, 16.5);

    // minHorizontal, maxHorizontal
    DataFrame dfMin = df.select(minHorizontal(col("A"), col("B")).alias("min_h"));
    assertColumnValues(dfMin, "min_h", 1, 2, 3);

    DataFrame dfMax = df.select(maxHorizontal(col("A"), col("B")).alias("max_h"));
    assertColumnValues(dfMax, "max_h", 10, 20, 30);

    // boolean horizontals
    DataFrame dfBase = intFrame("ColX", 2, 2, 3, 1);
    DataFrame dfBool =
        dfBase
            .withColumn("A_bool", col("ColX").equalTo(2)) // [true, true, false, false]
            .withColumn("B_bool", col("ColX").greaterThanEqualTo(2)); // [true, true, true, false]

    DataFrame dfAny = dfBool.select(anyHorizontal(col("A_bool"), col("B_bool")).alias("any_h"));
    assertColumnValues(dfAny, "any_h", true, true, true, false);

    DataFrame dfAll = dfBool.select(allHorizontal(col("A_bool"), col("B_bool")).alias("all_h"));
    assertColumnValues(dfAll, "all_h", true, true, false, false);
  }
}
