package org.polars.scala.polars.api

import org.polars.scala.polars.internal.jni.series

class Series private (private[polars] val ptr: Long) {

  def show(): Unit = series.show(ptr)
}

object Series {
  private final val EmptyString = ""

  def of(name: String, data: Array[Series]): Series =
    Series.withPtr(series.new_list_series(name, data.map(_.ptr)))

  def of(name: String, data: Array[Int]): Series =
    Series.withPtr(series.new_int_series(name, data))

  def of(name: String, data: Array[Long]): Series =
    Series.withPtr(series.new_long_series(name, data))

  def of(name: String, data: Array[Float]): Series =
    Series.withPtr(series.new_float_series(name, data))

  def of(name: String, data: Array[Double]): Series =
    Series.withPtr(series.new_double_series(name, data))

  def of(name: String, data: Array[Boolean]): Series =
    Series.withPtr(series.new_boolean_series(name, data))

  def of(name: String, data: Array[String]): Series =
    Series.withPtr(series.new_str_series(name, data))

  def of(name: String, data: Array[java.time.LocalDate]): Series =
    Series.withPtr(
      series.new_date_series(
        name,
        data.map(v => java.time.format.DateTimeFormatter.ISO_LOCAL_DATE.format(v))
      )
    )

  def of(name: String, data: Array[java.time.LocalDateTime]): Series =
    Series.withPtr(
      series.new_datetime_series(
        name,
        data.map(v => java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(v))
      )
    )

  def of[T](name: String, data: Array[Array[T]]): Series =
    Series.of(
      name,
      data.map {
        case e: Array[Int] => Series.of(EmptyString, e)
        case e: Array[Long] => Series.of(EmptyString, e)
        case e: Array[Float] => Series.of(EmptyString, e)
        case e: Array[Double] => Series.of(EmptyString, e)
        case e: Array[Boolean] => Series.of(EmptyString, e)
        case e: Array[java.time.LocalDate] => Series.of(EmptyString, e)
        case e: Array[java.time.LocalDateTime] => Series.of(EmptyString, e)
        case e: Array[String] => Series.of(EmptyString, e)
        case e: Array[Array[_]] => Series.of(EmptyString, e)
        case _ =>
          throw new IllegalArgumentException(
            s"List Series of provided type `${data.getClass.getSimpleName}` is currently not supported."
          )
      }
    )

  def withPtr(ptr: Long) = new Series(ptr)
}
