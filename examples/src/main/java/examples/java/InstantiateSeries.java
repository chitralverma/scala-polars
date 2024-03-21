package examples.java;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import org.polars.scala.polars.api.Series;

public class InstantiateSeries {
  public static void main(String[] args) {

    // Primitives
    Series series_str = Series.of("series_str", new String[] {"a", "b"});
    Series series_i32_1 = Series.of("series_i32_1", new int[] {1, 2, 3});
    Series series_i32_2 = Series.of("series_i32_2", new Integer[] {1, 2, 3});
    Series series_i64_1 = Series.of("series_i64_1", new long[] {1L, 2L});
    Series series_i64_2 = Series.of("series_i64_2", new Long[] {1L, 2L});
    Series series_f32_1 = Series.of("series_f32_1", new float[] {1.1f, 2.2f, 3.4f});
    Series series_f32_2 = Series.of("series_f32_2", new Float[] {1.1f, 2.2f, 3.4f});
    Series series_f64_1 = Series.of("series_f64_1", new double[] {1.1d, 2.2d, 3.4d});
    Series series_f64_2 = Series.of("series_f64_2", new Double[] {1.1d, 2.2d, 3.4d});
    Series series_bool_1 = Series.of("series_bool_1", new boolean[] {true, false, true, true});
    Series series_bool_2 = Series.of("series_bool_2", new Boolean[] {true, false, true, true});
    Series series_date =
        Series.of("series_date", new java.time.LocalDate[] {java.time.LocalDate.now()});
    Series series_datetime =
        Series.of("series_datetime", new java.time.LocalDateTime[] {java.time.LocalDateTime.now()});

    //     List Types
    Series series_list_str = Series.of("series_list_str", Arrays.asList(Arrays.asList("a", "b")));
    Series series_list_i32 = Series.of("series_list_i32", Arrays.asList(Arrays.asList(1, 2, 3)));

    Series series_list_i64 = Series.of("series_list_i64", Arrays.asList(Arrays.asList(1L, 2L, 3L)));
    Series series_list_f32 =
        Series.of("series_list_f32", Arrays.asList(Arrays.asList(1.1f, 2.2f, 3.4f)));
    Series series_list_f64 =
        Series.of("series_list_f64", Arrays.asList(Arrays.asList(1.1d, 2.2d, 3.4d)));
    Series series_list_bool =
        Series.of("series_list_bool", Arrays.asList(Arrays.asList(true, false, true, true)));
    Series series_list_date =
        Series.of("series_list_date", Arrays.asList(Arrays.asList(LocalDate.now())));
    Series series_list_datetime =
        Series.of("series_list_datetime", Arrays.asList(Arrays.asList(LocalDateTime.now())));
    //    Series series_list_list_str =
    //        Series.of("series_list_list_str", Arrays.asList(Arrays.asList(Arrays.asList("a",
    // "b")))); // todo fix this

    // Empty Series
    Series series_empty_str = Series.of("series_empty_str", new String[] {});
    Series series_empty_i32_1 = Series.of("series_empty_i32_1", new int[] {});
    Series series_empty_i32_2 = Series.of("series_empty_i32_2", new Integer[] {});
    Series series_empty_i64_1 = Series.of("series_empty_i64_1", new long[] {});
    Series series_empty_i64_2 = Series.of("series_empty_i64_2", new Long[] {});
    Series series_empty_f32_1 = Series.of("series_empty_f32_1", new float[] {});
    Series series_empty_f32_2 = Series.of("series_empty_f32_2", new Float[] {});
    Series series_empty_f64_1 = Series.of("series_empty_f64_1", new double[] {});
    Series series_empty_f64_2 = Series.of("series_empty_f64_2", new Double[] {});
    Series series_empty_bool_1 = Series.of("series_empty_bool_1", new boolean[] {});
    Series series_empty_bool_2 = Series.of("series_empty_bool_2", new Boolean[] {});
    Series series_empty_date = Series.of("series_empty_date", new java.time.LocalDate[] {});
    Series series_empty_datetime =
        Series.of("series_empty_datetime", new java.time.LocalDateTime[] {});

    Series[] seriesArr = {
      series_str,
      series_i32_1,
      series_i32_2,
      series_i64_1,
      series_i64_2,
      series_f32_1,
      series_f32_2,
      series_f64_1,
      series_f64_2,
      series_bool_1,
      series_bool_2,
      series_date,
      series_datetime,
      series_list_str,
      series_list_i32,
      series_list_i64,
      series_list_f32,
      series_list_f64,
      series_list_bool,
      series_list_date,
      series_list_datetime,
      //      series_list_list_str,
      series_empty_str,
      series_empty_i32_1,
      series_empty_i32_2,
      series_empty_i64_1,
      series_empty_i64_2,
      series_empty_f32_1,
      series_empty_f32_2,
      series_empty_f64_1,
      series_empty_f64_2,
      series_empty_bool_1,
      series_empty_bool_2,
      series_empty_date,
      series_empty_datetime,
    };

    for (Series series : seriesArr) {
      series.show();
    }
  }
}
