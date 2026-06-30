package com.github.chitralverma.polars

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime, ZonedDateTime}

import com.github.chitralverma.polars.api.expressions.{Column, Expression}
import com.github.chitralverma.polars.internal.jni.expressions.{column_expr, literal_expr}

object functions {

  def col(name: String): Column = Column.from(name)

  def lit(value: Any): Expression = value match {
    case v: Expression => v
    case _ =>
      val ptr = value match {
        case null => literal_expr.nullLit()
        case v: Boolean => literal_expr.fromBool(v)
        case v: Int => literal_expr.fromInt(v)
        case v: Long => literal_expr.fromLong(v)
        case v: Float => literal_expr.fromFloat(v)
        case v: Double => literal_expr.fromDouble(v)
        case v: LocalDate =>
          literal_expr.fromDate(DateTimeFormatter.ISO_LOCAL_DATE.format(v))
        case v: LocalTime =>
          literal_expr.fromTime(DateTimeFormatter.ISO_LOCAL_TIME.format(v))
        case v: ZonedDateTime =>
          literal_expr.fromDateTime(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(v))
        case v: String => literal_expr.fromString(v)
        case _ =>
          throw new IllegalArgumentException(
            s"Unsupported value `$value` of type `${value.getClass.getSimpleName}` was provided."
          )
      }

      Expression.withPtr(ptr)
  }

  def desc(col_name: String): Expression = {
    val ptr = column_expr.sortColumnByName(col_name, descending = true)
    Expression.withPtr(ptr)
  }

  def asc(col_name: String): Expression = {
    val ptr = column_expr.sortColumnByName(col_name, descending = false)
    Expression.withPtr(ptr)
  }

  // --- Core Expression Aggregations (JVM-friendly overloads) ---

  def sum(col: Expression): Column = col.sum()
  def sum(colName: String): Column = col(colName).sum()

  def min(col: Expression): Column = col.min()
  def min(colName: String): Column = col(colName).min()

  def max(col: Expression): Column = col.max()
  def max(colName: String): Column = col(colName).max()

  def mean(col: Expression): Column = col.mean()
  def mean(colName: String): Column = col(colName).mean()

  def median(col: Expression): Column = col.median()
  def median(colName: String): Column = col(colName).median()

  def std(col: Expression, ddof: Int): Column = col.std(ddof)
  def std(col: Expression): Column = col.std()
  def std(colName: String, ddof: Int): Column = col(colName).std(ddof)
  def std(colName: String): Column = col(colName).std()

  def `var`(col: Expression, ddof: Int): Column = col.`var`(ddof)
  def `var`(col: Expression): Column = col.`var`()
  def `var`(colName: String, ddof: Int): Column = col(colName).`var`(ddof)
  def `var`(colName: String): Column = col(colName).`var`()

  def variance(col: Expression, ddof: Int): Column = col.variance(ddof)
  def variance(col: Expression): Column = col.variance()
  def variance(colName: String, ddof: Int): Column = col(colName).variance(ddof)
  def variance(colName: String): Column = col(colName).variance()

  def product(col: Expression): Column = col.product()
  def product(colName: String): Column = col(colName).product()

  def count(col: Expression): Column = col.count()
  def count(colName: String): Column = col(colName).count()

  def len(): Column = col("*").len()
  def len(col: Expression): Column = col.len()
  def len(colName: String): Column = col(colName).len()

  def nUnique(col: Expression): Column = col.nUnique()
  def nUnique(colName: String): Column = col(colName).nUnique()

  def approxNUnique(col: Expression): Column = col.approxNUnique()
  def approxNUnique(colName: String): Column = col(colName).approxNUnique()

  def first(col: Expression): Column = col.first()
  def first(colName: String): Column = col(colName).first()

  def last(col: Expression): Column = col.last()
  def last(colName: String): Column = col(colName).last()

  def quantile(col: Expression, q: Double, method: String): Column = col.quantile(q, method)
  def quantile(col: Expression, q: Double): Column = col.quantile(q)
  def quantile(colName: String, q: Double, method: String): Column =
    col(colName).quantile(q, method)
  def quantile(colName: String, q: Double): Column = col(colName).quantile(q)

  def any(col: Expression, ignoreNulls: Boolean): Column = col.any(ignoreNulls)
  def any(col: Expression): Column = col.any()
  def any(colName: String, ignoreNulls: Boolean): Column = col(colName).any(ignoreNulls)
  def any(colName: String): Column = col(colName).any()

  def all(col: Expression, ignoreNulls: Boolean): Column = col.all(ignoreNulls)
  def all(col: Expression): Column = col.all()
  def all(colName: String, ignoreNulls: Boolean): Column = col(colName).all(ignoreNulls)
  def all(colName: String): Column = col(colName).all()

  def cumSum(col: Expression, reverse: Boolean): Column = col.cumSum(reverse)
  def cumSum(col: Expression): Column = col.cumSum()
  def cumSum(colName: String, reverse: Boolean): Column = col(colName).cumSum(reverse)
  def cumSum(colName: String): Column = col(colName).cumSum()

  // --- Horizontal Aggregations ---

  import com.github.chitralverma.polars.internal.jni.expressions.functions_expr

  @annotation.varargs
  def anyHorizontal(cols: Expression*): Column =
    Column.withPtr(functions_expr.anyHorizontal(cols.map(_.ptr).toArray))

  @annotation.varargs
  def allHorizontal(cols: Expression*): Column =
    Column.withPtr(functions_expr.allHorizontal(cols.map(_.ptr).toArray))

  @annotation.varargs
  def maxHorizontal(cols: Expression*): Column =
    Column.withPtr(functions_expr.maxHorizontal(cols.map(_.ptr).toArray))

  @annotation.varargs
  def minHorizontal(cols: Expression*): Column =
    Column.withPtr(functions_expr.minHorizontal(cols.map(_.ptr).toArray))

  @annotation.varargs
  def sumHorizontal(cols: Expression*): Column =
    Column.withPtr(functions_expr.sumHorizontal(cols.map(_.ptr).toArray, true))

  @annotation.varargs
  def sumHorizontal(ignoreNulls: Boolean, cols: Expression*): Column =
    Column.withPtr(functions_expr.sumHorizontal(cols.map(_.ptr).toArray, ignoreNulls))

  @annotation.varargs
  def meanHorizontal(cols: Expression*): Column =
    Column.withPtr(functions_expr.meanHorizontal(cols.map(_.ptr).toArray, true))

  @annotation.varargs
  def meanHorizontal(ignoreNulls: Boolean, cols: Expression*): Column =
    Column.withPtr(functions_expr.meanHorizontal(cols.map(_.ptr).toArray, ignoreNulls))

}
