package com.github.chitralverma.polars.api

import java.util.Collections

import scala.annotation.varargs
import scala.jdk.CollectionConverters._

import com.github.chitralverma.polars.api.expressions.Expression
import com.github.chitralverma.polars.api.types.Schema
import com.github.chitralverma.polars.config.UniqueKeepStrategies
import com.github.chitralverma.polars.internal.jni.expressions.column_expr
import com.github.chitralverma.polars.internal.jni.lazy_frame

class LazyFrame private (private[polars] val _ptr: Long) extends AutoCloseable {

  private var isClosed = false

  private[polars] def ptr: Long = {
    checkClosed()
    _ptr
  }

  override def close(): Unit = synchronized {
    if (!isClosed && _ptr != 0) {
      lazy_frame.free(_ptr)
      isClosed = true
    }
  }

  override def finalize(): Unit = close()

  private def checkClosed(): Unit =
    if (isClosed) throw new IllegalStateException("LazyFrame is already closed.")

  val schema: Schema = {
    val schemaString = lazy_frame.schemaString(_ptr)
    Schema.fromString(schemaString)
  }

  val width: Int = schema.getFields.length

  @varargs
  def select(colName: String, colNames: String*): LazyFrame = {
    val ldfPtr = lazy_frame.selectFromStrings(ptr, colNames.+:(colName).distinct.toArray)

    LazyFrame.withPtr(ldfPtr)
  }

  @varargs
  def select(column: Expression, columns: Expression*): LazyFrame = {
    val ldfPtr = lazy_frame.selectFromExprs(ptr, columns.+:(column).map(_.ptr).distinct.toArray)

    LazyFrame.withPtr(ldfPtr)
  }

  def filter(predicate: Expression): LazyFrame = {
    val ldfPtr = lazy_frame.filterFromExprs(ptr, predicate.ptr)

    LazyFrame.withPtr(ldfPtr)
  }

  def sort(
      cols: Array[String],
      descending: Array[Boolean],
      nullLast: Array[Boolean],
      maintainOrder: Boolean
  ): LazyFrame = {
    assert(
      cols.length == descending.length,
      s"Length of provided list columns(${cols.length}) and their " +
        s"sorting directions((${descending.length})) is not equal."
    )

    val exprs = cols.zip(descending).map { case (column, bool) =>
      Expression.withPtr(column_expr.sort_column_by_name(column, bool))
    }

    sort(exprs, nullLast, maintainOrder = maintainOrder)
  }

  def sort(
      col: String,
      descending: Boolean,
      nullLast: Boolean,
      maintainOrder: Boolean
  ): LazyFrame =
    sort(Array(col), Array(descending), Array(nullLast), maintainOrder = maintainOrder)

  def sort(
      exprs: Array[Expression],
      null_last: Array[Boolean],
      maintainOrder: Boolean
  ): LazyFrame = {
    assert(
      exprs.length == null_last.length,
      s"Length of provided expressions (${exprs.length}) and their " +
        s"null_last (${null_last.length}) is not equal."
    )

    val ldfPtr =
      lazy_frame.sortFromExprs(ptr, exprs.map(_.ptr).distinct, null_last, maintainOrder)

    LazyFrame.withPtr(ldfPtr)
  }

  def sort(expr: Expression, nullLast: Boolean, maintainOrder: Boolean): LazyFrame =
    sort(Array(expr), Array(nullLast), maintainOrder = maintainOrder)

  def set_sorted(mapping: Map[String, Boolean]): LazyFrame =
    set_sorted(mapping.asJava)

  def set_sorted(mapping: java.util.Map[String, Boolean]): LazyFrame = {
    val ldfPtr = lazy_frame.set_sorted(ptr, mapping)

    LazyFrame.withPtr(ldfPtr)
  }

  def top_k(
      k: Int,
      exprs: Array[Expression],
      null_last: Array[Boolean],
      maintainOrder: Boolean
  ): LazyFrame = {
    assert(
      exprs.length == null_last.length,
      s"Length of provided expressions (${exprs.length}) and their " +
        s"null_last (${null_last.length}) is not equal."
    )
    val ldfPtr =
      lazy_frame.topKFromExprs(ptr, k, exprs.map(_.ptr).distinct, null_last, maintainOrder)

    LazyFrame.withPtr(ldfPtr)
  }

  def top_k(k: Int, expr: Expression, nullLast: Boolean, maintainOrder: Boolean): LazyFrame =
    top_k(k, Array(expr), Array(nullLast), maintainOrder = maintainOrder)

  def top_k(
      k: Int,
      cols: Array[String],
      descending: Array[Boolean],
      nullLast: Array[Boolean],
      maintainOrder: Boolean
  ): LazyFrame = {
    assert(
      cols.length == descending.length,
      s"Length of provided list columns (${cols.length}) and their " +
        s"sorting directions (${descending.length}) is not equal."
    )

    val exprs = cols.zip(descending).map { case (column, bool) =>
      Expression.withPtr(column_expr.sort_column_by_name(column, bool))
    }

    top_k(k, exprs, null_last = nullLast, maintainOrder = maintainOrder)
  }

  def top_k(
      k: Int,
      col: String,
      descending: Boolean,
      nullLast: Boolean,
      maintainOrder: Boolean
  ): LazyFrame =
    top_k(k, Array(col), Array(descending), Array(nullLast), maintainOrder = maintainOrder)

  def limit(n: Long): LazyFrame =
    LazyFrame.withPtr(lazy_frame.limit(ptr, n))

  def head(n: Long): LazyFrame = limit(n)

  def first(): LazyFrame = limit(1)

  def tail(n: Long): LazyFrame =
    LazyFrame.withPtr(lazy_frame.tail(ptr, n))

  def last(): LazyFrame = tail(1)

  @varargs
  def drop(colName: String, colNames: String*): LazyFrame = {
    val ldfPtr = lazy_frame.drop(ptr, colNames.+:(colName).distinct.toArray)

    LazyFrame.withPtr(ldfPtr)
  }

  def with_column(name: String, expr: Expression): LazyFrame = {
    val ldfPtr = lazy_frame.withColumn(ptr, name, expr.ptr)

    LazyFrame.withPtr(ldfPtr)
  }

  def rename(oldName: String, newName: String): LazyFrame =
    rename(Collections.singletonMap(oldName, newName))

  def rename(mapping: Map[String, String]): LazyFrame = rename(mapping.asJava)

  def rename(mapping: java.util.Map[String, String]): LazyFrame = {
    val ldfPtr = lazy_frame.rename(ptr, mapping)

    LazyFrame.withPtr(ldfPtr)
  }

  def unique: LazyFrame = unique()

  def unique(
      subset: Array[String] = Array.empty,
      keep: UniqueKeepStrategies.UniqueKeepStrategy = UniqueKeepStrategies.any,
      maintainOrder: Boolean = false
  ): LazyFrame = {
    val ldfPtr = lazy_frame.unique(ptr, subset, keep.toString, maintainOrder)

    LazyFrame.withPtr(ldfPtr)
  }

  def drop_nulls: LazyFrame = drop_nulls()

  def drop_nulls(
      subset: Array[String] = Array.empty
  ): LazyFrame = {
    val ldfPtr = lazy_frame.drop_nulls(ptr, subset)

    LazyFrame.withPtr(ldfPtr)
  }

  def explain: Unit = explain()

  def explain(
      optimized: Boolean = true,
      typeCoercion: Boolean = true,
      predicatePushdown: Boolean = true,
      projectionPushdown: Boolean = true,
      simplifyExpression: Boolean = true,
      slicePushdown: Boolean = true,
      commSubplanElim: Boolean = true,
      commSubexprElim: Boolean = true,
      streaming: Boolean = false,
      treeFormat: Boolean = false
  ): Unit = {
    val planStr = if (optimized) {
      lazy_frame.explain(
        lazy_frame.optimization_toggle(
          ptr,
          typeCoercion = typeCoercion,
          predicatePushdown = predicatePushdown,
          projectionPushdown = projectionPushdown,
          simplifyExpr = simplifyExpression,
          slicePushdown = slicePushdown,
          commSubplanElim = commSubplanElim,
          commSubexprElim = commSubexprElim,
          streaming = streaming
        ),
        optimized = true,
        treeFormat
      )
    } else lazy_frame.explain(ptr, optimized = false, treeFormat)

    println(planStr)
  }

  def cache: LazyFrame = {
    val ldfPtr = lazy_frame.cache(ptr)

    LazyFrame.withPtr(ldfPtr)
  }

  def collect: DataFrame = collect()

  def collect(
      typeCoercion: Boolean = true,
      predicatePushdown: Boolean = true,
      projectionPushdown: Boolean = true,
      simplifyExpression: Boolean = true,
      noOptimization: Boolean = false,
      slicePushdown: Boolean = true,
      commSubplanElim: Boolean = true,
      commSubexprElim: Boolean = true,
      streaming: Boolean = false
  ): DataFrame = {
    val ldf = LazyFrame.withPtr(
      lazy_frame.optimization_toggle(
        ptr,
        typeCoercion = typeCoercion,
        predicatePushdown = if (noOptimization) false else predicatePushdown,
        projectionPushdown = if (noOptimization) false else projectionPushdown,
        simplifyExpr = simplifyExpression,
        slicePushdown = if (noOptimization) false else slicePushdown,
        commSubplanElim = if (noOptimization || streaming) false else commSubplanElim,
        commSubexprElim = if (noOptimization) false else commSubexprElim,
        streaming = streaming
      )
    )

    val dfPtr = lazy_frame.collect(ldf.ptr)
    DataFrame.withPtr(dfPtr)
  }

}

object LazyFrame {

  private[polars] def withPtr(ptr: Long) = new LazyFrame(ptr)

}
