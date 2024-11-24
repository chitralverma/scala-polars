package examples.scala

import org.polars.scala.polars.api.Series

object InstantiateSeries {

  def main(args: Array[String]): Unit = {

    /* Values as Scala array/ iterable of Basic Types */

    // Int
    Series.ofInt("series_i32_scala_array", Array(1, 2, 3)).show()
    Series.ofInt("series_i32_scala_iterable", Seq(1, 2, 3)).show()

    // Long
    Series.ofLong("series_i64_scala_array", Array(1L, 2L, 3L)).show()
    Series.ofLong("series_i64_scala_iterable", Seq(1L, 2L, 3L)).show()

    // Float
    Series.ofFloat("series_f32_scala_array", Array(1f, 2f, 3f)).show()
    Series.ofFloat("series_f32_scala_iterable", Seq(1f, 2f, 3f)).show()

    // Double
    Series.ofDouble("series_f64_scala_array", Array(1d, 2d, 3d)).show()
    Series.ofDouble("series_f64_scala_iterable", Seq(1d, 2d, 3d)).show()

    // Boolean
    Series.ofBoolean("series_bool_scala_array", Array(true, false, true, true)).show()
    Series.ofBoolean("series_bool_scala_iterable", Seq(true, false, true, true)).show()

    // String
    Series.ofString("series_str_scala_array", Array("a", "b")).show()
    Series.ofString("series_str_scala_iterable", Seq("a", "b")).show()

    // java.time.LocalDate
    Series.ofDate("series_date_scala_array", Array(java.time.LocalDate.now())).show()
    Series.ofDate("series_date_scala_iterable", Seq(java.time.LocalDate.now())).show()

    // java.time.LocalTime
    Series.ofTime("series_time_scala_array", Array(java.time.LocalTime.now())).show()
    Series.ofTime("series_time_scala_iterable", Seq(java.time.LocalTime.now())).show()

    // java.time.ZonedDateTime
    Series.ofDateTime("series_datetime_scala_array", Array(java.time.ZonedDateTime.now())).show()
    Series.ofDateTime("series_datetime_scala_iterable", Seq(java.time.ZonedDateTime.now())).show()

    /* Values as Java array/ list of Nested List Types */

    // int or java.lang.Integer
    Series.ofList("series_list_int_scala_array", Array(Array(1, 2, 3))).show()
    Series.ofList("series_list_int_scala_iterable", Seq(Seq(1, 2, 3))).show()

    // String
    Series.ofList("series_list_str_scala_array", Array(Array("a", "b"))).show()
    Series.ofList("series_list_str_scala_iterable", Seq(Seq("a", "b"))).show()

    // Deep Nested
    Series.ofList("series_list_list_str_scala_array", Array(Array(Array("a", "b")))).show()
    Series.ofList("series_list_list_str_scala_iterable", Seq(Seq(Seq("a", "b")))).show()

    /* Values as Java array/ list of Struct Types */
    Series
      .ofSeries(
        "series_struct_scala_array",
        Array(
          Series.ofInt("int_col", Array(1, 2, 3)),
          Series.ofString("str_col", Array("a", "b", "c")),
          Series.ofBoolean("bool_col", Array(true, false, true))
        )
      )
      .show()
    Series
      .ofSeries(
        "series_struct_scala_iterable",
        Seq(
          Series.ofInt("int_col", Seq(1, 2, 3)),
          Series.ofString("str_col", Seq("a", "b", "c")),
          Series.ofBoolean("bool_col", Seq(true, false, true))
        )
      )
      .show()
  }

}
