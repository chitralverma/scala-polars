package examples.scala

import com.github.chitralverma.polars.api.{DataFrame, Series}

object InstantiateDataFrame {

  def main(args: Array[String]): Unit = {
    DataFrame.fromSeries(Series.ofBoolean("bool_col", Array[Boolean](true, false, true))).show()

    DataFrame
      .fromSeries(
        Series.ofInt("i32_col", Array[Int](1, 2, 3)),
        Series.ofLong("i64_col", Array[Long](1L, 2L, 3L)),
        Series.ofBoolean("bool_col", Array[Boolean](true, false, true)),
        Series.ofList(
          "nested_str_col",
          Array[Array[String]](Array("a", "b", "c"), Array("a", "b", "c"), Array("a", "b", "c"))
        )
      )
      .show()

    /* Values as Scala array(s) */
    DataFrame
      .fromSeries(
        Series.ofInt("i32_col", Array[Int](1, 2, 3)),
        Array[Series](
          Series.ofLong("i64_col", Array[Long](1L, 2L, 3L)),
          Series.ofBoolean("bool_col", Array[Boolean](true, false, true)),
          Series.ofList(
            "nested_str_col",
            Array[Array[String]](Array("a", "b", "c"), Array("a", "b", "c"), Array("a", "b", "c"))
          )
        )
      )
      .show()

    /* Values as scala lists(s) */

    DataFrame
      .fromSeries(
        Series.ofInt("i32_col", Seq(1, 2, 3)),
        Array[Series](
          Series.ofLong("i64_col", Seq(1L, 2L, 3L)),
          Series.ofBoolean("bool_col", Seq(true, false, true)),
          Series.ofFloat("f32_col", Seq(1f, 2f, 3f))
        )
      )
      .show()

    /* Values as a mix of Scala lists(s) and array(s) */

    DataFrame
      .fromSeries(
        Series.ofInt("i32_col", Seq(1, 2, 3)),
        Array[Series](
          Series.ofLong("i64_col", Array[Long](1L, 2L, 3L)),
          Series.ofBoolean("bool_col", Array[Boolean](true, false, true)),
          Series.ofFloat("f32_col", Seq(1f, 2f, 3f))
        )
      )
      .show()

    DataFrame
      .fromSeries(
        Series.ofInt("i32_col", Array[Int](1, 2, 3)),
        Series.ofLong("i64_col", Array[Long](1L, 2L, 3L)),
        Series.ofBoolean("bool_col", Array[Boolean](true, false, true)),
        Series.ofSeries(
          "struct_col",
          Array[Series](
            Series.ofLong("i64_col", Array[Long](1L, 2L, 3L)),
            Series.ofBoolean("bool_col", Array[Boolean](true, false, true)),
            Series.ofFloat("f32_col", Seq(1f, 2f, 3f))
          )
        )
      )
      .show()
  }
}
