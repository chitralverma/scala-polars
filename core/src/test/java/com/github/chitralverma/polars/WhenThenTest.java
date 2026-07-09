package com.github.chitralverma.polars;

import static com.github.chitralverma.polars.functions.*;

import com.github.chitralverma.polars.api.DataFrame;
import com.github.chitralverma.polars.api.Series;
import com.github.chitralverma.polars.testing.AbstractPolarsJavaTest;
import org.junit.Test;

/**
 * Java mirror of {@code WhenThenSuite}, testing the {@code when / otherwise} conditional builder.
 */
public class WhenThenTest extends AbstractPolarsJavaTest {

  @Test
  public void basicBranchAndImplicitNullDefault() {
    DataFrame df = intFrame("a", 1, 2, 3, 4, 5);

    DataFrame withOtherwise = df.select(when(col("a").lessThan(3), "x").otherwise("y").alias("a"));
    assertColumnValues(withOtherwise, "a", "x", "x", "y", "y", "y");

    DataFrame implicitNull = df.select(when(col("a").lessThan(3), "x").alias("b"));
    assertColumnValues(implicitNull, "b", "x", "x", null, null, null);
  }

  @Test
  public void chainedConditions() {
    DataFrame df = intFrame("a", 1, 2, 3, 4, 5);

    DataFrame withOtherwise =
        df.select(
            when(col("a").lessThan(3), "x")
                .when(col("a").greaterThan(4), "z")
                .otherwise("y")
                .alias("a"));
    assertColumnValues(withOtherwise, "a", "x", "x", "y", "y", "z");

    DataFrame implicitNull =
        df.select(when(col("a").lessThan(3), "x").when(col("a").greaterThan(4), "z").alias("b"));
    assertColumnValues(implicitNull, "b", "x", "x", null, null, "z");
  }

  @Test
  public void bareWhenUsableAsExpression() {
    DataFrame df =
        DataFrame.fromSeries(
            Series.ofString("team", new String[] {"A", "A", "A", "B", "B", "C"}),
            Series.ofInt("points", new int[] {11, 8, 10, 6, 6, 5}));

    DataFrame result = df.select(when(col("points").greaterThan(7), "Foo").alias("flag"));
    assertColumnValues(result, "flag", "Foo", "Foo", "Foo", null, null, null);
  }

  @Test
  public void valueTakenFromAnotherColumn() {
    DataFrame df =
        DataFrame.fromSeries(
            Series.ofInt("x", new int[] {0, 1, 2, 3, 4}),
            Series.ofInt("y", new int[] {5, 6, 7, 8, 9}));

    DataFrame result =
        df.select(when(col("x").lessThan(2), col("x")).otherwise(col("y")).alias("out"));
    assertColumnValues(result, "out", 0, 1, 7, 8, 9);
  }

  @Test
  public void stringSupertypeCoercion() {
    DataFrame df =
        DataFrame.fromSeries(
            Series.ofString("names", new String[] {"foo", "spam", "spam"}),
            Series.ofInt("nrs", new int[] {1, 2, 3}));

    DataFrame result =
        df.select(
            when(col("names").equalTo("spam"), col("nrs").multiply(2))
                .otherwise(lit("other"))
                .alias("new_col"));
    assertColumnValues(result, "new_col", "other", "4", "6");
  }

  @Test
  public void numericSupertypeCascade() {
    DataFrame df = intFrame("foo", 1, 3, 4);

    DataFrame result =
        df.select(
            when(col("foo").equalTo(1), 1)
                .when(col("foo").equalTo(2), 4)
                .when(col("foo").equalTo(3), 1.5)
                .when(col("foo").equalTo(4), 16)
                .otherwise(0)
                .alias("val"));
    assertColumnValues(result, "val", 1.0, 1.5, 16.0);
  }

  @Test
  public void longChainFoldsCorrectly() {
    DataFrame df = intFrame("n", 1, 2, 3, 4, 5, 6, 7);

    DataFrame result =
        df.select(
            when(col("n").equalTo(1), "one")
                .when(col("n").equalTo(2), "two")
                .when(col("n").equalTo(3), "three")
                .when(col("n").equalTo(4), "four")
                .when(col("n").equalTo(5), "five")
                .when(col("n").equalTo(6), "six")
                .otherwise("many")
                .alias("word"));
    assertColumnValues(result, "word", "one", "two", "three", "four", "five", "six", "many");
  }

  @Test
  public void nestedViaAnyHorizontalEqualsChained() {
    DataFrame df =
        DataFrame.fromSeries(
            Series.ofString("c1", new String[] {"a", "b"}),
            Series.ofString("c2", new String[] {"c", "d"}));

    DataFrame nested =
        df.select(
            when(anyHorizontal(col("c1").equalTo("a"), col("c2").equalTo("a")), "a")
                .otherwise(
                    when(anyHorizontal(col("c1").equalTo("d"), col("c2").equalTo("d")), "d")
                        .otherwise(lit(null)))
                .alias("result"));

    DataFrame chained =
        df.select(
            when(anyHorizontal(col("c1").equalTo("a"), col("c2").equalTo("a")), "a")
                .when(anyHorizontal(col("c1").equalTo("d"), col("c2").equalTo("d")), "d")
                .otherwise(lit(null))
                .alias("result"));

    assertColumnValues(nested, "result", "a", "d");
    assertColumnValues(chained, "result", "a", "d");
  }
}
