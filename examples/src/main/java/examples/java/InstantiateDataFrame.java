package examples.java;

import java.util.Arrays;
import org.polars.scala.polars.api.DataFrame;
import org.polars.scala.polars.api.Series;

public class InstantiateDataFrame {

  public static void main(String[] args) {
    DataFrame.fromSeries(Series.ofBoolean("bool_col", new boolean[] {true, false, true})).show();

    DataFrame.fromSeries(
            Series.ofInt("i32_col", new int[] {1, 2, 3}),
            Series.ofLong("i64_col", new long[] {1L, 2L, 3L}),
            Series.ofBoolean("bool_col", new boolean[] {true, false, true}),
            Series.ofList(
                "nested_str_col",
                new String[][] {
                  {"a", "b", "c"},
                  {"a", "b", "c"},
                  {"a", "b", "c"},
                }))
        .show();

    /* Values as Java array(s) */

    DataFrame.fromSeries(
            Series.ofInt("i32_col", new int[] {1, 2, 3}),
            new Series[] {
              Series.ofLong("i64_col", new long[] {1L, 2L, 3L}),
              Series.ofBoolean("bool_col", new boolean[] {true, false, true}),
              Series.ofList(
                  "nested_str_col",
                  new String[][] {
                    {"a", "b", "c"},
                    {"a", "b", "c"},
                    {"a", "b", "c"},
                  }),
            })
        .show();

    DataFrame.fromSeries(
            Series.ofInt("i32_col", new Integer[] {1, 2, 3}),
            new Series[] {
              Series.ofLong("i64_col", new Long[] {1L, 2L, 3L}),
              Series.ofBoolean("bool_col", new Boolean[] {true, false, true}),
              Series.ofFloat("f32_col", new Float[] {1F, 2F, 3F}),
            })
        .show();

    /* Values as Java lists(s) */

    DataFrame.fromSeries(
            Series.ofInt("i32_col", Arrays.asList(1, 2, 3)),
            new Series[] {
              Series.ofLong("i64_col", Arrays.asList(1L, 2L, 3L)),
              Series.ofBoolean("bool_col", Arrays.asList(true, false, true)),
              Series.ofFloat("f32_col", Arrays.asList(1F, 2F, 3F)),
            })
        .show();

    /* Values as a mix of Java lists(s) and array(s) */

    DataFrame.fromSeries(
            Series.ofInt("i32_col", Arrays.asList(1, 2, 3)),
            new Series[] {
              Series.ofLong("i64_col", new Long[] {1L, 2L, 3L}),
              Series.ofBoolean("bool_col", new Boolean[] {true, false, true}),
              Series.ofFloat("f32_col", Arrays.asList(1F, 2F, 3F)),
            })
        .show();

    DataFrame.fromSeries(
            Series.ofInt("i32_col", Arrays.asList(1, 2, 3)),
            new Series[] {
              Series.ofLong("i64_col", new Long[] {1L, 2L, 3L}),
              Series.ofBoolean("bool_col", new Boolean[] {true, false, true}),
              Series.ofSeries(
                  "struct_col",
                  new Series[] {
                    Series.ofLong("i64_col", new Long[] {1L, 2L, 3L}),
                    Series.ofBoolean("bool_col", new Boolean[] {true, false, true}),
                    Series.ofFloat("f32_col", Arrays.asList(1F, 2F, 3F)),
                  }),
            })
        .show();
  }
}
