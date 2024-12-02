package org.polars.scala.polars

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime, ZonedDateTime}

import org.polars.scala.polars.api.expressions.{Column, Expression}
import org.polars.scala.polars.internal.jni.expressions.{column_expr, literal_expr}

object functions {

  def col(name: String): Column = Column.from(name)

  def lit(value: Any): Expression = {
    val ptr = value match {
      case null => literal_expr.nullLit()
      case v: Expression => v.ptr
      case v: Boolean => literal_expr.fromBool(v)
      case v: Int => literal_expr.fromInt(v)
      case v: Long => literal_expr.fromLong(v)
      case v: Float => literal_expr.fromFloat(v)
      case v: Double => literal_expr.fromDouble(v)
      case v: LocalDate =>
        literal_expr.fromDate(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE.format(v))
      case v: LocalTime =>
        literal_expr.fromTime(java.time.format.DateTimeFormatter.ISO_LOCAL_TIME.format(v))
      case v: ZonedDateTime =>
        literal_expr.fromDateTime(
          java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(v)
        )
      case v: String => literal_expr.fromString(v)
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported value `$value` of type `${value.getClass.getSimpleName}` was provided."
        )
    }

    Expression.withPtr(ptr)
  }

  def desc(col_name: String): Expression = {
    val ptr = column_expr.sort_column_by_name(col_name, descending = true)
    Expression.withPtr(ptr)
  }

  def asc(col_name: String): Expression = {
    val ptr = column_expr.sort_column_by_name(col_name, descending = false)
    Expression.withPtr(ptr)
  }

}
