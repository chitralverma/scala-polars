package com.github.chitralverma.polars.api

import java.util.Collections

import scala.annotation.varargs
import scala.jdk.CollectionConverters._

import com.github.chitralverma.polars.api.expressions.Expression
import com.github.chitralverma.polars.api.io.Writeable
import com.github.chitralverma.polars.api.types.Schema
import com.github.chitralverma.polars.config.UniqueKeepStrategies
import com.github.chitralverma.polars.internal.jni.data_frame

class DataFrame private (private[polars] val _ptr: Long) extends AutoCloseable {

  private var isClosed = false

  private[polars] def ptr: Long = {
    checkClosed()
    _ptr
  }

  override def close(): Unit = synchronized {
    if (!isClosed && _ptr != 0) {
      data_frame.free(_ptr)
      isClosed = true
    }
  }

  override def finalize(): Unit = close()

  private[polars] def checkClosed(): Unit =
    if (isClosed) throw new IllegalStateException("DataFrame is already closed.")

  lazy val schema: Schema = {
    val schemaString = data_frame.schemaString(ptr)
    Schema.fromString(schemaString)
  }

  lazy val width: Int = schema.getFields.length

  lazy val height: Long = count()

  lazy val shape: (Long, Int) = (height, width)

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
      nullLast: Array[Boolean],
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
        nullLast = Array(nullLast),
        maintainOrder = maintainOrder
      )
      .collect(noOptimization = true)

  def sort(
      exprs: Array[Expression],
      null_last: Array[Boolean],
      maintain_order: Boolean
  ): DataFrame =
    toLazy.sort(exprs, null_last, maintain_order).collect(noOptimization = true)

  def sort(expr: Expression, null_last: Boolean, maintain_order: Boolean): DataFrame =
    toLazy
      .sort(Array(expr), Array(null_last), maintainOrder = maintain_order)
      .collect(noOptimization = true)

  def setSorted(
      column: String,
      descending: Boolean = false,
      nullsLast: Boolean = false
  ): DataFrame =
    toLazy.setSorted(column, descending, nullsLast).collect(noOptimization = true)

  def topK(
      k: Int,
      cols: Array[String],
      descending: Array[Boolean],
      nullLast: Array[Boolean],
      maintainOrder: Boolean
  ): DataFrame =
    toLazy
      .topK(k, cols, descending, nullLast, maintainOrder)
      .collect(projectionPushdown = false, predicatePushdown = false, commSubplanElim = false)

  def topK(
      k: Int,
      expr: String,
      descending: Boolean,
      nullLast: Boolean,
      maintainOrder: Boolean
  ): DataFrame =
    toLazy
      .topK(
        k = k,
        cols = Array(expr),
        descending = Array(descending),
        nullLast = Array(nullLast),
        maintainOrder = maintainOrder
      )
      .collect(projectionPushdown = false, predicatePushdown = false, commSubplanElim = false)

  def topK(
      k: Int,
      exprs: Array[Expression],
      nullLast: Array[Boolean],
      maintainOrder: Boolean
  ): DataFrame =
    toLazy
      .topK(k, exprs, nullLast, maintainOrder)
      .collect(projectionPushdown = false, predicatePushdown = false, commSubplanElim = false)

  def topK(k: Int, expr: Expression, nullLast: Boolean, maintainOrder: Boolean): DataFrame =
    toLazy
      .topK(k, Array(expr), Array(nullLast), maintainOrder = maintainOrder)
      .collect(projectionPushdown = false, predicatePushdown = false, commSubplanElim = false)

  def limit(n: Long): DataFrame =
    DataFrame.withPtr(data_frame.limit(ptr, n))

  def head(n: Long): DataFrame = limit(n)

  def first(): DataFrame = limit(1)

  def tail(n: Long): DataFrame =
    DataFrame.withPtr(data_frame.tail(ptr, n))

  def last(): DataFrame = tail(1)

  def withColumn(name: String, expr: Expression): DataFrame =
    toLazy.withColumn(name, expr).collect(noOptimization = true)

  @varargs
  def drop(colName: String, colNames: String*): DataFrame =
    toLazy.drop(colName, colNames: _*).collect(noOptimization = true)

  def dropNulls: DataFrame = dropNulls()

  def dropNulls(
      subset: Array[String] = Array.empty
  ): DataFrame =
    toLazy.dropNulls(subset).collect(noOptimization = true)

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

  def toLazy: LazyFrame =
    LazyFrame.withPtr(data_frame.toLazy(ptr))

  def show(): Unit =
    data_frame.show(ptr)

  def count(): Long =
    data_frame.count(ptr)

  /** Provides an iterator to traverse a specified number of rows from the DataFrame.
    * @param nRows
    *   number of rows to traverse
    * @note
    *   if `nRows` is greater than the total rows in DataFrame then all rows are included.
    * @return
    *   Iterator of [[Row]]
    */
  def rows(nRows: Long): Iterator[Row] =
    RowIterator.withPtr(ptr, this).lazyIterator(nRows)

  /** Provides an iterator to traverse a all rows from the DataFrame.
    * @return
    *   Iterator of [[Row]]
    */
  def rows(): Iterator[Row] = rows(-1L)

  def write(): Writeable =
    new Writeable(this)

}

object DataFrame {

  private[polars] def withPtr(ptr: Long) = new DataFrame(ptr)

  /** Initialize new [[com.github.chitralverma.polars.api.DataFrame]] from one or more
    * [[com.github.chitralverma.polars.api.Series]]. The name of a series is used as column name
    * and its values are the values of this column.
    *
    * @param series
    *   Series
    * @param more
    *   Series as a scala or java array
    *
    * @return
    *   [[com.github.chitralverma.polars.api.DataFrame]] formed from the provided
    *   [[com.github.chitralverma.polars.api.Series]]
    */
  @varargs
  def fromSeries(series: Series, more: Series*): DataFrame =
    DataFrame.withPtr(data_frame.fromSeries(more.+:(series).map(_.ptr).toArray))

  /** Initialize new [[com.github.chitralverma.polars.api.DataFrame]] from one or more
    * [[com.github.chitralverma.polars.api.Series]]. The name of a series is used as column name
    * and its values are the values of this column.
    *
    * @param series
    *   Series
    * @param more
    *   Series as a scala iterable
    *
    * @return
    *   [[com.github.chitralverma.polars.api.DataFrame]] formed from the provided
    *   [[com.github.chitralverma.polars.api.Series]]
    */
  def fromSeries(series: Series, more: Iterable[Series]): DataFrame =
    DataFrame.withPtr(data_frame.fromSeries(more.toSeq.+:(series).map(_.ptr).toArray))

  /** Initialize new [[com.github.chitralverma.polars.api.DataFrame]] from one or more
    * [[com.github.chitralverma.polars.api.Series]]. The name of a series is used as column name
    * and its values are the values of this column.
    *
    * @param series
    *   Series
    * @param more
    *   Series as a java iterable
    *
    * @return
    *   [[com.github.chitralverma.polars.api.DataFrame]] formed from the provided
    *   [[com.github.chitralverma.polars.api.Series]]
    */
  def fromSeries(series: Series, more: java.lang.Iterable[Series]): DataFrame =
    fromSeries(series, more.asScala)

}
