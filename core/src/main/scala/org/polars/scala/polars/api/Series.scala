package org.polars.scala.polars.api

import scala.jdk.CollectionConverters._

import org.polars.scala.polars.internal.jni.series

class Series private (private[polars] val ptr: Long) {

  def show(): Unit = series.show(ptr)
}

object Series {

  /** Initialize new series by name and values of type [[scala.Int]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[scala.Int]]. If `values` is empty, empty series is returned retaining
    *   type.
    */
  def ofInt(name: String, values: Array[Int]): Series =
    Series.withPtr(series.new_int_series(name, values))

  /** Initialize new series by name and values of type [[scala.Int]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala iterable
    *
    * @return
    *   Series of type [[scala.Int]]. If `values` is empty, empty series is returned retaining
    *   type.
    */
  def ofInt(name: String, values: Iterable[Int]): Series = Series.ofInt(name, values.toArray)

  /** Initialize new series by name and values of type [[java.lang.Integer]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[java.lang.Integer]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofInt(name: String, values: Array[java.lang.Integer]): Series =
    Series.ofInt(name, values.map(_.intValue()))

  /** Initialize new series by name and values of type [[java.lang.Integer]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a java iterable
    *
    * @return
    *   Series of type [[java.lang.Integer]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofInt(name: String, values: java.lang.Iterable[java.lang.Integer]): Series =
    Series.ofInt(name, values.asScala.toArray)

  /** Initialize new series by name and values of type [[scala.Long]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[scala.Long]]. If `values` is empty, empty series is returned retaining
    *   type.
    */
  def ofLong(name: String, values: Array[Long]): Series =
    Series.withPtr(series.new_long_series(name, values))

  /** Initialize new series by name and values of type [[scala.Long]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala iterable
    *
    * @return
    *   Series of type [[scala.Long]]. If `values` is empty, empty series is returned retaining
    *   type.
    */
  def ofLong(name: String, values: Iterable[Long]): Series = Series.ofLong(name, values.toArray)

  /** Initialize new series by name and values of type [[java.lang.Long]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[java.lang.Long]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofLong(name: String, values: Array[java.lang.Long]): Series =
    Series.ofLong(name, values.map(_.longValue()))

  /** Initialize new series by name and values of type [[java.lang.Long]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a java iterable
    *
    * @return
    *   Series of type [[java.lang.Long]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofLong(name: String, values: java.lang.Iterable[java.lang.Long]): Series =
    Series.ofLong(name, values.asScala.toArray)

  /** Initialize new series by name and values of type [[scala.Float]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[scala.Float]]. If `values` is empty, empty series is returned retaining
    *   type.
    */
  def ofFloat(name: String, values: Array[Float]): Series =
    Series.withPtr(series.new_float_series(name, values))

  /** Initialize new series by name and values of type [[scala.Float]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala iterable
    *
    * @return
    *   Series of type [[scala.Float]]. If `values` is empty, empty series is returned retaining
    *   type.
    */
  def ofFloat(name: String, values: Iterable[Float]): Series =
    Series.ofFloat(name, values.toArray)

  /** Initialize new series by name and values of type [[java.lang.Float]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[java.lang.Float]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofFloat(name: String, values: Array[java.lang.Float]): Series =
    Series.ofFloat(name, values.map(_.floatValue()))

  /** Initialize new series by name and values of type [[java.lang.Float]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a java iterable
    *
    * @return
    *   Series of type [[java.lang.Float]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofFloat(name: String, values: java.lang.Iterable[java.lang.Float]): Series =
    Series.ofFloat(name, values.asScala.toArray)

  /** Initialize new series by name and values of type [[scala.Double]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[scala.Double]]. If `values` is empty, empty series is returned retaining
    *   type.
    */
  def ofDouble(name: String, values: Array[Double]): Series =
    Series.withPtr(series.new_double_series(name, values))

  /** Initialize new series by name and values of type [[scala.Double]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala iterable
    *
    * @return
    *   Series of type [[scala.Double]]. If `values` is empty, empty series is returned retaining
    *   type.
    */
  def ofDouble(name: String, values: Iterable[Double]): Series =
    Series.ofDouble(name, values.toArray)

  /** Initialize new series by name and values of type [[java.lang.Double]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[java.lang.Double]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofDouble(name: String, values: Array[java.lang.Double]): Series =
    Series.ofDouble(name, values.map(_.doubleValue()))

  /** Initialize new series by name and values of type [[java.lang.Double]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a java iterable
    *
    * @return
    *   Series of type [[java.lang.Double]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofDouble(name: String, values: java.lang.Iterable[java.lang.Double]): Series =
    Series.ofDouble(name, values.asScala.toArray)

  /** Initialize new series by name and values of type [[scala.Boolean]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[scala.Boolean]]. If `values` is empty, empty series is returned retaining
    *   type.
    */
  def ofBoolean(name: String, values: Array[Boolean]): Series =
    Series.withPtr(series.new_boolean_series(name, values))

  /** Initialize new series by name and values of type [[scala.Boolean]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala iterable
    *
    * @return
    *   Series of type [[scala.Boolean]]. If `values` is empty, empty series is returned retaining
    *   type.
    */
  def ofBoolean(name: String, values: Iterable[Boolean]): Series =
    Series.ofBoolean(name, values.toArray)

  /** Initialize new series by name and values of type [[java.lang.Boolean]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[java.lang.Boolean]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofBoolean(name: String, values: Array[java.lang.Boolean]): Series =
    Series.ofBoolean(name, values.map(_.booleanValue()))

  /** Initialize new series by name and values of type [[java.lang.Boolean]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a java iterable
    *
    * @return
    *   Series of type [[java.lang.Boolean]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofBoolean(name: String, values: java.lang.Iterable[java.lang.Boolean]): Series =
    Series.ofBoolean(name, values.asScala.toArray)

  /** Initialize new series by name and values of type [[java.lang.String]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[java.lang.String]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofString(name: String, values: Array[String]): Series =
    Series.withPtr(series.new_str_series(name, values))

  /** Initialize new series by name and values of type [[java.lang.String]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala iterable
    *
    * @return
    *   Series of type [[java.lang.String]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofString(name: String, values: Iterable[String]): Series =
    Series.ofString(name, values.toArray)

  /** Initialize new series by name and values of type [[java.lang.String]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a java iterable
    *
    * @return
    *   Series of type [[java.lang.String]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofString(name: String, values: java.lang.Iterable[java.lang.String]): Series =
    Series.ofString(name, values.asScala.toArray)

  /** Initialize new series by name and values of type [[java.time.LocalTime]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[java.time.LocalTime]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofTime(name: String, values: Array[java.time.LocalTime]): Series =
    Series.withPtr(
      series.new_time_series(
        name,
        values.map(v => java.time.format.DateTimeFormatter.ISO_LOCAL_TIME.format(v))
      )
    )

  /** Initialize new series by name and values of type [[java.time.LocalTime]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala iterable
    *
    * @return
    *   Series of type [[java.time.LocalTime]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofTime(name: String, values: Iterable[java.time.LocalTime]): Series =
    Series.ofTime(name, values.toArray)

  /** Initialize new series by name and values of type [[java.time.LocalTime]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a java iterable
    *
    * @return
    *   Series of type [[java.time.LocalTime]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofTime(name: String, values: java.lang.Iterable[java.time.LocalTime]): Series =
    Series.ofTime(name, values.asScala.toArray)

  /** Initialize new series by name and values of type [[java.time.LocalDate]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[java.time.LocalDate]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofDate(name: String, values: Array[java.time.LocalDate]): Series =
    Series.withPtr(
      series.new_date_series(
        name,
        values.map(v => java.time.format.DateTimeFormatter.ISO_LOCAL_DATE.format(v))
      )
    )

  /** Initialize new series by name and values of type [[java.time.LocalDate]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala iterable
    *
    * @return
    *   Series of type [[java.time.LocalDate]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofDate(name: String, values: Iterable[java.time.LocalDate]): Series =
    Series.ofDate(name, values.toArray)

  /** Initialize new series by name and values of type [[java.time.LocalDate]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a java iterable
    *
    * @return
    *   Series of type [[java.time.LocalDate]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofDate(name: String, values: java.lang.Iterable[java.time.LocalDate]): Series =
    Series.ofDate(name, values.asScala.toArray)

  /** Initialize new series by name and values of type [[java.time.LocalDateTime]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[java.time.LocalDateTime]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofDateTime(name: String, values: Array[java.time.ZonedDateTime]): Series =
    Series.withPtr(
      series.new_datetime_series(
        name,
        values.map(v => java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(v))
      )
    )

  /** Initialize new series by name and values of type [[java.time.LocalDateTime]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala iterable
    *
    * @return
    *   Series of type [[java.time.LocalDateTime]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofDateTime(name: String, values: Iterable[java.time.ZonedDateTime]): Series =
    Series.ofDateTime(name, values.toArray)

  /** Initialize new series by name and values of type [[java.time.LocalDateTime]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a java iterable
    *
    * @return
    *   Series of type [[java.time.LocalDateTime]]. If `values` is empty, empty series is returned
    *   retaining type.
    */
  def ofDateTime(name: String, values: java.lang.Iterable[java.time.ZonedDateTime]): Series =
    Series.ofDateTime(name, values.asScala.toArray)

  /** Initialize new series (struct) by name and values of type [[Series]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Series of type [[Series]]. If `values` is empty, empty series is returned retaining type.
    */
  def ofSeries(name: String, values: Array[Series]): Series =
    Series.withPtr(series.new_struct_series(name, values.map(_.ptr)))

  /** Initialize new series (struct) by name and values of type [[Series]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala iterable
    *
    * @return
    *   Series of type [[Series]]. If `values` is empty, empty series is returned retaining type.
    */
  def ofSeries(name: String, values: Iterable[Series]): Series =
    Series.ofSeries(name, values.toArray)

  /** Initialize new series (struct) by name and values of type [[Series]].
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a java iterable
    *
    * @return
    *   Series of type [[Series]]. If `values` is empty, empty series is returned retaining type.
    */
  def ofSeries(name: String, values: java.lang.Iterable[Series]): Series =
    Series.ofSeries(name, values.asScala.toArray)

  /** Initialize new nested series by name and values of provided type.
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a java iterable
    *
    * @return
    *   Nested Series of provided type. If `values` is empty, empty series is returned retaining
    *   type. Nested collections in `values` must not be empty or this will result in
    *   [[java.lang.ArrayIndexOutOfBoundsException]].
    */
  def ofList(name: String, values: java.lang.Iterable[java.lang.Iterable[_]]): Series =
    JSeries.ofList(name, values)

  /** Initialize new nested series by name and values of provided type.
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala iterable
    *
    * @return
    *   Nested Series of provided type. If `values` is empty, empty series is returned retaining
    *   type. Nested collections in `values` must not be empty or this will result in
    *   [[java.lang.ArrayIndexOutOfBoundsException]].
    */
  def ofList(name: String, values: Iterable[Iterable[_]]): Series =
    Series.ofList(name, values.map(_.asJava.asInstanceOf[java.lang.Iterable[_]]).asJava)

  /** Initialize new nested series by name and values of provided type.
    *
    * @param name
    *   Name of Series
    * @param values
    *   Values of Series as a scala or java array
    *
    * @return
    *   Nested Series of provided type. If `values` is empty, empty series is returned retaining
    *   type. Nested collections in `values` must not be empty or this will result in
    *   [[java.lang.ArrayIndexOutOfBoundsException]].
    */
  def ofList[T](name: String, values: Array[Array[T]]): Series =
    Series.ofList(name, values.map(_.toSeq).toSeq)

  def withPtr(ptr: Long) = new Series(ptr)
}
