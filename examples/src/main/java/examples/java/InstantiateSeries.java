package examples.java;

import java.util.Arrays;
import java.util.Collections;
import org.polars.scala.polars.api.Series;

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

    // java.time.LocalDateTime
    Series.ofDateTime(
            "series_datetime_java_array_primitive",
            new java.time.LocalDateTime[] {java.time.LocalDateTime.now()})
        .show();
    Series.ofDateTime(
            "series_datetime_java_list", Collections.singletonList(java.time.LocalDateTime.now()))
        .show();

    /* Values as Java array/ list of Nested List Types */

    // int or java.lang.Integer
    Series.ofList("series_list_int_java_array", new java.lang.Integer[][] {{1, 2, 3}}).show();
    Series.ofList("series_list_int_java_list", Arrays.asList(Arrays.asList(1, 2, 3))).show();

    // String
    Series.ofList("series_list_str_java_array", new String[][] {{"a", "b"}}).show();
    Series.ofList("series_list_str_java_list", Arrays.asList(Arrays.asList("a", "b"))).show();

    // Deep Nested
    Series.ofList("series_list_list_str_java_array", new String[][][] {{{"a", "b"}}}).show();
    Series.ofList(
            "series_list_list_str_java_list", Arrays.asList(Arrays.asList(Arrays.asList("a", "b"))))
        .show();
  }
}
