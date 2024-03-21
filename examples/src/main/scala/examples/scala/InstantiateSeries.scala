package examples.scala

import java.time.{LocalDate, LocalDateTime}

import org.polars.scala.polars.api.Series

object InstantiateSeries {

  def main(args: Array[String]): Unit = {

    // Primitives
    val series_str = Series.of("series_str", Array("a", "b"))
    val series_i32 = Series.of("series_i32", Array(1, 2, 3))
    val series_i64 = Series.of("series_i64", Array(1L, 2L))
    val series_f32 = Series.of("series_f32", Array(1.1f, 2.2f, 3.4f))
    val series_f64 = Series.of("series_f64", Array(1.1d, 2.2d, 3.4d))
    val series_bool = Series.of("series_bool", Array(true, false, true, true))
    val series_date = Series.of("series_date", Array(java.time.LocalDate.now()))
    val series_datetime = Series.of("series_datetime", Array(java.time.LocalDateTime.now()))

    // List Types
    val series_list_str = Series.of("series_list_str", Array(Array("a", "b")))
    val series_list_i32 = Series.of("series_list_i32", Array(Array(1, 2, 3)))
    val series_list_i64 = Series.of("series_list_i64", Array(Array(1L, 2L, 3L)))
    val series_list_f32 = Series.of("series_list_f32", Array(Array(1.1f, 2.2f, 3.4f)))
    val series_list_f64 = Series.of("series_list_f64", Array(Array(1.1d, 2.2d, 3.4d)))
    val series_list_bool = Series.of("series_list_bool", Array(Array(true, false, true, true)))
    val series_list_date = Series.of("series_list_date", Array(Array(java.time.LocalDate.now())))
    val series_list_datetime =
      Series.of("series_list_datetime", Array(Array(java.time.LocalDateTime.now())))
    val series_list_list_str = Series.of("series_list_list_str", Array(Array(Array("a", "b"))))

    // Empty Series
    val series_empty_str = Series.of("series_empty_str", Array.empty[String])
    val series_empty_i32 = Series.of("series_empty_i32", Array.empty[Int])
    val series_empty_i64 = Series.of("series_empty_i64", Array.empty[Long])
    val series_empty_f32 = Series.of("series_empty_f32", Array.empty[Float])
    val series_empty_f64 = Series.of("series_empty_f64", Array.empty[Double])
    val series_empty_bool = Series.of("series_empty_bool", Array.empty[Boolean])
    val series_empty_date = Series.of("series_empty_date", Array.empty[LocalDate])
    val series_empty_datetime = Series.of("series_empty_datetime", Array.empty[LocalDateTime])
    val series_empty_list_str = Series.of("series_list_str", Array(Array.empty[String]))

    Array(
      series_str,
      series_i32,
      series_i64,
      series_f32,
      series_f64,
      series_bool,
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
      series_list_list_str,
      series_empty_str,
      series_empty_i32,
      series_empty_i64,
      series_empty_f32,
      series_empty_f64,
      series_empty_bool,
      series_empty_date,
      series_empty_datetime,
      series_empty_list_str
    ).foreach(
      _.show()
    )

  }

}
