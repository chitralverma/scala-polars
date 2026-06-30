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

  lazy val schema: Schema = {
    val schemaString = lazy_frame.schemaString(ptr)
    Schema.fromString(schemaString)
  }

  lazy val width: Int = schema.getFields.length

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
      Expression.withPtr(column_expr.sortColumnByName(column, bool))
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
      lazy_frame.sortFromExprs(ptr, exprs.map(_.ptr), null_last, maintainOrder)

    LazyFrame.withPtr(ldfPtr)
  }

  def sort(expr: Expression, nullLast: Boolean, maintainOrder: Boolean): LazyFrame =
    sort(Array(expr), Array(nullLast), maintainOrder = maintainOrder)

  def setSorted(
      column: String,
      descending: Boolean = false,
      nullsLast: Boolean = false
  ): LazyFrame = {
    val ldfPtr = lazy_frame.setSorted(ptr, column, descending, nullsLast)

    LazyFrame.withPtr(ldfPtr)
  }

  def topK(
      k: Int,
      exprs: Array[Expression],
      nullLast: Array[Boolean],
      maintainOrder: Boolean
  ): LazyFrame = {
    assert(
      exprs.length == nullLast.length,
      s"Length of provided expressions (${exprs.length}) and their " +
        s"nullLast (${nullLast.length}) is not equal."
    )
    val ldfPtr =
      lazy_frame.topKFromExprs(ptr, k, exprs.map(_.ptr), nullLast, maintainOrder)

    LazyFrame.withPtr(ldfPtr)
  }

  def topK(k: Int, expr: Expression, nullLast: Boolean, maintainOrder: Boolean): LazyFrame =
    topK(k, Array(expr), Array(nullLast), maintainOrder = maintainOrder)

  def topK(
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
      Expression.withPtr(column_expr.sortColumnByName(column, bool))
    }

    topK(k, exprs, nullLast = nullLast, maintainOrder = maintainOrder)
  }

  def topK(
      k: Int,
      col: String,
      descending: Boolean,
      nullLast: Boolean,
      maintainOrder: Boolean
  ): LazyFrame =
    topK(k, Array(col), Array(descending), Array(nullLast), maintainOrder = maintainOrder)

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

  def withColumn(name: String, expr: Expression): LazyFrame = {
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

  def dropNulls: LazyFrame = dropNulls()

  def dropNulls(
      subset: Array[String] = Array.empty
  ): LazyFrame = {
    val ldfPtr = lazy_frame.dropNulls(ptr, subset)

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
        lazy_frame.optimizationToggle(
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
      lazy_frame.optimizationToggle(
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
