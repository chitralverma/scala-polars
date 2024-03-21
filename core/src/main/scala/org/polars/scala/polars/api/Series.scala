package org.polars.scala.polars.api

import scala.jdk.CollectionConverters._
import scala.util.Try

import org.polars.scala.polars.internal.jni.series

import izumi.reflect._

class Series private (private[polars] val ptr: Long) {

  def show(): Unit = series.show(ptr)
}

object Series {
  private final val EmptyString = ""

  /** Initialize new nested series by name and values of type [[Series]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Nested Series of type [[Series]]. If `values` is empty, empty series is returned retaining
    *   type.
    */
  def of(name: String, values: Array[Series]): Series =
    Series.withPtr(series.new_list_series(name, values.map(_.ptr)))

  /** Initialize new series by name and values of type [[Int]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Series of type [[Int]]. If `values` is empty, empty series is returned retaining type.
    */
  def of(name: String, values: Array[Int]): Series =
    Series.withPtr(series.new_int_series(name, values))

  /** Initialize new series by name and values of type [[java.lang.Integer]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Series of type [[java.lang.Integer]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def of(name: String, values: Array[java.lang.Integer]): Series =
    Series.withPtr(series.new_int_series(name, values.map(_.intValue())))

  /** Initialize new series by name and values of type [[Long]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Series of type [[Long]]. If `values` is empty, empty series is returned retaining type.
    */
  def of(name: String, values: Array[Long]): Series =
    Series.withPtr(series.new_long_series(name, values))

  /** Initialize new series by name and values of type [[java.lang.Long]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Series of type [[java.lang.Long]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def of(name: String, values: Array[java.lang.Long]): Series =
    Series.withPtr(series.new_long_series(name, values.map(_.longValue())))

  /** Initialize new series by name and values of type [[Float]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Series of type [[Float]]. If `values` is empty, empty series is returned retaining type.
    */
  def of(name: String, values: Array[Float]): Series =
    Series.withPtr(series.new_float_series(name, values))

  /** Initialize new series by name and values of type [[java.lang.Float]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Series of type [[java.lang.Float]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def of(name: String, values: Array[java.lang.Float]): Series =
    Series.withPtr(series.new_float_series(name, values.map(_.floatValue())))

  /** Initialize new series by name and values of type [[Double]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Series of type [[Double]]. If `values` is empty, empty series is returned retaining type.
    */
  def of(name: String, values: Array[Double]): Series =
    Series.withPtr(series.new_double_series(name, values))

  /** Initialize new series by name and values of type [[java.lang.Double]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Series of type [[java.lang.Double]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def of(name: String, values: Array[java.lang.Double]): Series =
    Series.withPtr(series.new_double_series(name, values.map(_.doubleValue())))

  /** Initialize new series by name and values of type [[Boolean]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Series of type [[Boolean]]. If `values` is empty, empty series is returned retaining type.
    */
  def of(name: String, values: Array[Boolean]): Series =
    Series.withPtr(series.new_boolean_series(name, values))

  /** Initialize new series by name and values of type [[java.lang.Boolean]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Series of type [[java.lang.Boolean]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def of(name: String, values: Array[java.lang.Boolean]): Series =
    Series.withPtr(series.new_boolean_series(name, values.map(_.booleanValue())))

  /** Initialize new series by name and values of type [[String]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Series of type [[String]]. If `values` is empty, empty series is returned retaining type.
    */
  def of(name: String, values: Array[String]): Series =
    Series.withPtr(series.new_str_series(name, values))

  /** Initialize new series by name and values of type [[java.time.LocalDate]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Series of type [[java.time.LocalDate]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def of(name: String, values: Array[java.time.LocalDate]): Series =
    Series.withPtr(
      series.new_date_series(
        name,
        values.map(v => java.time.format.DateTimeFormatter.ISO_LOCAL_DATE.format(v))
      )
    )

  /** Initialize new series by name and values of type [[java.time.LocalDateTime]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Series of type [[java.time.LocalDateTime]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def of(name: String, values: Array[java.time.LocalDateTime]): Series =
    Series.withPtr(
      series.new_datetime_series(
        name,
        values.map(v => java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(v))
      )
    )

  /** Initialize new nested series by name and values of provided type.
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series
    *
    * @return
    *   Nested Series of provided type. If `values` is empty, empty series is returned retaining
    *   type.
    */
  def of[T: Tag](name: String, values: Array[Array[T]]): Series =
    Series.of(
      name,
      Try(values.map {
        case e: Array[Int] => Series.of(EmptyString, e)
        case e: Array[java.lang.Integer] => Series.of(EmptyString, e)
        case e: Array[Long] => Series.of(EmptyString, e)
        case e: Array[java.lang.Long] => Series.of(EmptyString, e)
        case e: Array[Float] => Series.of(EmptyString, e)
        case e: Array[java.lang.Float] => Series.of(EmptyString, e)
        case e: Array[Double] => Series.of(EmptyString, e)
        case e: Array[java.lang.Double] => Series.of(EmptyString, e)
        case e: Array[Boolean] => Series.of(EmptyString, e)
        case e: Array[java.lang.Boolean] => Series.of(EmptyString, e)
        case e: Array[java.time.LocalDate] => Series.of(EmptyString, e)
        case e: Array[java.time.LocalDateTime] => Series.of(EmptyString, e)
        case e: Array[String] => Series.of(EmptyString, e)
        case e: Array[Array[_]] => Series.of(EmptyString, e)
      }).getOrElse(
        throw new IllegalArgumentException(
          s"Nested series of provided internal type `${Tag[T].closestClass.getSimpleName}` is currently not supported."
        )
      )
    )

  /** Initialize new nested series by name and values of provided type.
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a [[java.util.List]]
    *
    * @return
    *   Nested Series of provided type. If `values` is empty, empty series is returned retaining
    *   type.
    */
  def of[T](name: String, values: java.util.List[java.util.List[T]]): Series = {

    val data = values.asScala.toArray.map(_.toArray)

    Series.of(
      name,
      Try(
        data.map(_.asInstanceOf[Array[_]]).map {
          case e: Array[Int] => Series.of(EmptyString, e)
          case e: Array[java.lang.Integer] => Series.of(EmptyString, e)
          case e: Array[Long] => Series.of(EmptyString, e)
          case e: Array[java.lang.Long] => Series.of(EmptyString, e)
          case e: Array[Float] => Series.of(EmptyString, e)
          case e: Array[java.lang.Float] => Series.of(EmptyString, e)
          case e: Array[Double] => Series.of(EmptyString, e)
          case e: Array[java.lang.Double] => Series.of(EmptyString, e)
          case e: Array[Boolean] => Series.of(EmptyString, e)
          case e: Array[java.lang.Boolean] => Series.of(EmptyString, e)
          case e: Array[java.time.LocalDate] => Series.of(EmptyString, e)
          case e: Array[java.time.LocalDateTime] => Series.of(EmptyString, e)
          case e: Array[String] => Series.of(EmptyString, e)
          case e: Array[Array[_]] => Series.of(EmptyString, e)
        }
      ).getOrElse(
        throw new IllegalArgumentException(
          s"Nested series of provided internal type `${data.getClass.getSimpleName}` is currently not supported."
        )
      )
    )
  }

  def withPtr(ptr: Long) = new Series(ptr)
}
