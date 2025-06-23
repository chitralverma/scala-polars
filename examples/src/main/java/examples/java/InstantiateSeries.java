package examples.java;

import com.github.chitralverma.polars.api.Series;
import java.util.Arrays;
import java.util.Collections;

public class InstantiateSeries {
  public static void main(String[] args) {

    /* Values as Java array/ list of Basic Types */

    // int or java.lang.Integer
    Series.ofInt("series_i32_java_array_primitive", new int[] {1, 2, 3}).show();
    Series.ofInt("series_i32_java_array", new java.lang.Integer[] {1, 2, 3}).show();
    Series.ofInt("series_i32_java_list", Arrays.asList(1, 2, 3)).show();

    // long or java.lang.Long
    Series.ofLong("series_i64_java_array_primitive", new long[] {1L, 2L, 3L}).show();
    Series.ofLong("series_i64_java_array", new java.lang.Long[] {1L, 2L, 3L}).show();
    Series.ofLong("series_i64_java_list", Arrays.asList(1L, 2L, 3L)).show();

    // float or java.lang.Float
    Series.ofFloat("series_f32_java_array_primitive", new float[] {1f, 2f, 3f}).show();
    Series.ofFloat("series_f32_java_array", new java.lang.Float[] {1f, 2f, 3f}).show();
    Series.ofFloat("series_f32_java_list", Arrays.asList(1f, 2f, 3f)).show();

    // double or java.lang.Double
    Series.ofDouble("series_f64_java_array_primitive", new double[] {1d, 2d, 3d}).show();
    Series.ofDouble("series_f64_java_array", new java.lang.Double[] {1d, 2d, 3d}).show();
    Series.ofDouble("series_f64_java_list", Arrays.asList(1d, 2d, 3d)).show();

    // boolean or java.lang.Boolean
    Series.ofBoolean("series_bool_java_array_primitive", new boolean[] {true, false, true, true})
        .show();
    Series.ofBoolean("series_bool_java_array", new java.lang.Boolean[] {true, false, true, true})
        .show();
    Series.ofBoolean("series_bool_java_list", Arrays.asList(true, false, true, true)).show();

    // String
    Series.ofString("series_str_java_array_primitive", new String[] {"a", "b"}).show();
    Series.ofString("series_str_java_list", Arrays.asList("a", "b")).show();

    // java.time.LocalDate
    Series.ofDate(
            "series_date_java_array_primitive",
            new java.time.LocalDate[] {java.time.LocalDate.now()})
        .show();
    Series.ofDate("series_date_java_list", Collections.singletonList(java.time.LocalDate.now()))
        .show();

    // java.time.LocalTime
    Series.ofTime(
            "series_time_java_array_primitive",
            new java.time.LocalTime[] {java.time.LocalTime.now()})
        .show();
    Series.ofTime("series_time_java_list", Collections.singletonList(java.time.LocalTime.now()))
        .show();

    // java.time.ZonedDateTime
    Series.ofDateTime(
            "series_datetime_java_array_primitive",
            new java.time.ZonedDateTime[] {java.time.ZonedDateTime.now()})
        .show();
    Series.ofDateTime(
            "series_datetime_java_list", Collections.singletonList(java.time.ZonedDateTime.now()))
        .show();

    /* Values as Java array/ list of Nested List Types */

    // int or java.lang.Integer
    Series.ofList("series_list_int_java_array", new java.lang.Integer[][] {{1, 2, 3}}).show();
    Series.ofList("series_list_int_java_list", Collections.singletonList(Arrays.asList(1, 2, 3)))
        .show();

    // String
    Series.ofList("series_list_str_java_array", new String[][] {{"a", "b"}}).show();
    Series.ofList("series_list_str_java_list", Collections.singletonList(Arrays.asList("a", "b")))
        .show();

    // Deep Nested
    Series.ofList("series_list_list_str_java_array", new String[][][] {{{"a", "b"}}}).show();
    Series.ofList(
            "series_list_list_str_java_list",
            Collections.singletonList(Collections.singletonList(Arrays.asList("a", "b"))))
        .show();

    /* Values as Java array/ list of Struct Types */

    Series.ofSeries(
            "series_struct_java_array",
            new Series[] {
              Series.ofInt("int_col", new int[] {1, 2, 3}),
              Series.ofString("str_col", new String[] {"a", "b", "c"}),
              Series.ofBoolean("bool_col", new boolean[] {true, false, true}),
            })
        .show();
    Series.ofSeries(
            "series_struct_java_list",
            Arrays.asList(
                Series.ofInt("int_col", Arrays.asList(1, 2, 3)),
                Series.ofString("str_col", Arrays.asList("a", "b", "c")),
                Series.ofBoolean("bool_col", Arrays.asList(true, false, true))))
        .show();
  }
}
