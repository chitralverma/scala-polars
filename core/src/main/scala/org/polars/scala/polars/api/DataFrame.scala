package org.polars.scala.polars.api

import java.util
import java.util.Collections

import scala.annotation.varargs
import scala.jdk.CollectionConverters._

import org.polars.scala.polars.api.expressions.Expression
import org.polars.scala.polars.api.io.Writeable
import org.polars.scala.polars.api.types.Schema
import org.polars.scala.polars.config.UniqueKeepStrategies
import org.polars.scala.polars.internal.jni.data_frame

class DataFrame private (private[polars] val ptr: Long) {

  val schema: Schema = {
    val schemaString = data_frame.schemaString(ptr)
    Schema.from(schemaString)
  }

  val width: Int = schema.getFields.length

  val height: Long = count()

  val shape: (Long, Int) = (height, width)

  @varargs
  def select(colName: String, colNames: String*): DataFrame =
    toLazy.select(colName, colNames: _*).collect(noOptimization = true)

  @varargs
  def select(column: Expression, columns: Expression*): DataFrame =
    toLazy.select(column, columns: _*).collect(noOptimization = true)

  def filter(predicate: Expression): DataFrame =
    toLazy.filter(predicate).collect(noOptimization = true)

  def sort(
      cols: Array[String],
      descending: Array[Boolean],
      nullLast: Boolean,
      maintainOrder: Boolean
  ): DataFrame =
    toLazy.sort(cols, descending, nullLast, maintainOrder).collect(noOptimization = true)

  def sort(
      expr: String,
      descending: Boolean,
      nullLast: Boolean,
      maintainOrder: Boolean
  ): DataFrame =
    toLazy
      .sort(
        cols = Array(expr),
        descending = Array(descending),
        nullLast = nullLast,
        maintainOrder = maintainOrder
      )
      .collect(noOptimization = true)

  def sort(exprs: Array[Expression], null_last: Boolean, maintain_order: Boolean): DataFrame =
    toLazy.sort(exprs, null_last, maintain_order).collect(noOptimization = true)

  def sort(expr: Expression, null_last: Boolean, maintain_order: Boolean): DataFrame =
    toLazy
      .sort(Array(expr), null_last = null_last, maintainOrder = maintain_order)
      .collect(noOptimization = true)

  def set_sorted(mapping: Map[String, Boolean]): DataFrame =
    set_sorted(mapping.asJava)

  def set_sorted(mapping: java.util.Map[String, Boolean]): DataFrame =
    toLazy.set_sorted(mapping).collect(noOptimization = true)

  def top_k(
      k: Int,
      cols: Array[String],
      descending: Array[Boolean],
      nullLast: Boolean,
      maintainOrder: Boolean
  ): DataFrame =
    toLazy
      .top_k(k, cols, descending, nullLast, maintainOrder)
      .collect(projectionPushdown = false, predicatePushdown = false, commSubplanElim = false)

  def top_k(
      k: Int,
      expr: String,
      descending: Boolean,
      nullLast: Boolean,
      maintainOrder: Boolean
  ): DataFrame =
    toLazy
      .top_k(
        k = k,
        cols = Array(expr),
        descending = Array(descending),
        nullLast = nullLast,
        maintainOrder = maintainOrder
      )
      .collect(projectionPushdown = false, predicatePushdown = false, commSubplanElim = false)

  def top_k(
      k: Int,
      exprs: Array[Expression],
      null_last: Boolean,
      maintain_order: Boolean
  ): DataFrame =
    toLazy
      .top_k(k, exprs, null_last, maintain_order)
      .collect(projectionPushdown = false, predicatePushdown = false, commSubplanElim = false)

  def top_k(k: Int, expr: Expression, null_last: Boolean, maintain_order: Boolean): DataFrame =
    toLazy
      .top_k(k, Array(expr), null_last = null_last, maintainOrder = maintain_order)
      .collect(projectionPushdown = false, predicatePushdown = false, commSubplanElim = false)

  def limit(n: Long): DataFrame = DataFrame.withPtr(data_frame.limit(ptr, n))

  def head(n: Long): DataFrame = limit(n)

  def first(): DataFrame = limit(1)

  def tail(n: Long): DataFrame = DataFrame.withPtr(data_frame.tail(ptr, n))

  def last(): DataFrame = tail(1)

  def with_column(name: String, expr: Expression): DataFrame =
    toLazy.with_column(name, expr).collect(noOptimization = true)

  @varargs
  def drop(colName: String, colNames: String*): DataFrame =
    toLazy.drop(colName, colNames: _*).collect(noOptimization = true)

  def drop_nulls: DataFrame = drop_nulls()

  def drop_nulls(
      subset: Array[String] = Array.empty
  ): DataFrame =
    toLazy.drop_nulls(subset).collect(noOptimization = true)

  def rename(oldName: String, newName: String): DataFrame =
    rename(Collections.singletonMap(oldName, newName))

  def rename(mapping: Map[String, String]): DataFrame =
    rename(mapping.asJava)

  def rename(mapping: java.util.Map[String, String]): DataFrame =
    toLazy.rename(mapping).collect(noOptimization = true)

  def unique: DataFrame = unique()

  def unique(
      subset: Array[String] = Array.empty,
      keep: UniqueKeepStrategies.UniqueKeepStrategy = UniqueKeepStrategies.any,
      maintainOrder: Boolean = false
  ): DataFrame =
    toLazy.unique(subset, keep, maintainOrder).collect(noOptimization = true)

  def toLazy: LazyFrame = LazyFrame.withPtr(data_frame.toLazy(ptr))

  def show(): Unit = data_frame.show(ptr)

  def count(): Long = data_frame.count(ptr)

  def write(): Writeable = new Writeable(ptr)

}

object DataFrame {

  private[polars] def withPtr(ptr: Long) = new DataFrame(ptr)

  /** Initialize new [[org.polars.scala.polars.api.DataFrame]] from one or more
    * [[org.polars.scala.polars.api.Series]]. The name of a series is used as column name and its
    * values are the values of this column.
    *
    * @param series
    *   Series
    * @param more
    *   Series as a scala or java array
    *
    * @return
    *   [[org.polars.scala.polars.api.DataFrame]] formed from the provided
    *   [[org.polars.scala.polars.api.Series]]
    */
  def fromSeries(series: Series, more: Array[Series]): DataFrame =
    DataFrame.withPtr(data_frame.fromSeries(more.+:(series).map(_.ptr)))

  /** Initialize new [[org.polars.scala.polars.api.DataFrame]] from one or more
    * [[org.polars.scala.polars.api.Series]]. The name of a series is used as column name and its
    * values are the values of this column.
    *
    * @param series
    *   Series
    * @param more
    *   Series as a scala iterable
    *
    * @return
    *   [[org.polars.scala.polars.api.DataFrame]] formed from the provided
    *   [[org.polars.scala.polars.api.Series]]
    */
  def fromSeries(series: Series, more: Iterable[Series]): DataFrame =
    fromSeries(series, more.toArray)

  /** Initialize new [[org.polars.scala.polars.api.DataFrame]] from one or more
    * [[org.polars.scala.polars.api.Series]]. The name of a series is used as column name and its
    * values are the values of this column.
    *
    * @param series
    *   Series
    * @param more
    *   Series as a java iterable
    *
    * @return
    *   [[org.polars.scala.polars.api.DataFrame]] formed from the provided
    *   [[org.polars.scala.polars.api.Series]]
    */
  def fromSeries(series: Series, more: java.lang.Iterable[Series]): DataFrame =
    fromSeries(series, more.asScala)

  /** Initialize new [[org.polars.scala.polars.api.DataFrame]] from one or more
    * [[org.polars.scala.polars.api.Series]]. The name of a series is used as column name and its
    * values are the values of this column.
    *
    * @param series
    *   Series
    * @param more
    *   Series as scala varargs
    *
    * @return
    *   [[org.polars.scala.polars.api.DataFrame]] formed from the provided
    *   [[org.polars.scala.polars.api.Series]]
    */
  def fromSeries(series: Series, more: Series*): DataFrame =
    fromSeries(series, more)

}
