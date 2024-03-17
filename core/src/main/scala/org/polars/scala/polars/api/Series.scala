package org.polars.scala.polars.api

import scala.reflect.runtime.universe._

import org.polars.scala.polars.api.types._
import org.polars.scala.polars.internal.jni.series

class Series private (private[polars] val ptr: Long) {

  def show(): Unit = series.show(ptr)
}

object Series {

  def of[T: TypeTag](
      name: String,
      data: Array[T],
      dtype: Option[DataType] = None
  ): Series = {
    val ptr = data.headOption match {
      case Some(_: Int) => series.new_int_series(name, data.map(_.asInstanceOf[Int]))
      case Some(_: Long) => series.new_long_series(name, data.map(_.asInstanceOf[Long]))
      case Some(_: Float) => series.new_float_series(name, data.map(_.asInstanceOf[Float]))
      case Some(_: Double) => series.new_double_series(name, data.map(_.asInstanceOf[Double]))
      case Some(_: Boolean) => series.new_boolean_series(name, data.map(_.asInstanceOf[Boolean]))
      case Some(_: String) => series.new_str_series(name, data.map(_.asInstanceOf[String]))
      case None =>
        dtype match {
          case Some(dt) => empty(name, dt).ptr
          case None =>
            throw new IllegalArgumentException(
              "Empty `data` without `dtype` is not sufficient to form series. " +
                "To form empty series either use `Series.empty(...)` or provide `dtype`.."
            )
        }
      case _ =>
        throw new IllegalArgumentException(
          s"Series of data type `${typeOf[T].typeSymbol.name}` are currently not supported."
        )
    }

    Series.withPtr(ptr)
  }

  def empty(name: String, dtype: DataType): Series = {
    val ptr = dtype match {
      case IntType => series.new_int_series(name, Array.empty[Int])
      case LongType => series.new_long_series(name, Array.empty[Long])
      case FloatType => series.new_float_series(name, Array.empty[Float])
      case DoubleType => series.new_double_series(name, Array.empty[Double])
      case BooleanType => series.new_boolean_series(name, Array.empty[Boolean])
      case StringType => series.new_str_series(name, Array.empty[String])
      case t: DataType =>
        throw new IllegalArgumentException(
          s"Empty series of data type `${t.simpleName}` are currently not supported."
        )
    }

    Series.withPtr(ptr)
  }

  def withPtr(ptr: Long) = new Series(ptr)
}
