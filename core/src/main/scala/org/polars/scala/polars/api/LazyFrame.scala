package org.polars.scala.polars.api

import java.util.Collections

import scala.annotation.varargs
import scala.jdk.CollectionConverters._

import org.polars.scala.polars.api.expressions.Expression
import org.polars.scala.polars.api.types.Schema
import org.polars.scala.polars.internal.jni.expressions.column_expr
import org.polars.scala.polars.internal.jni.lazy_frame

class LazyFrame private (private[polars] val ptr: Long) {

  val schema: Schema = {
    val schemaString = lazy_frame.schemaString(ptr)
    Schema.from(schemaString)
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
      cols: Seq[String],
      descending: Seq[Boolean],
      nullLast: Boolean,
      maintainOrder: Boolean
  ): LazyFrame = {
    assert(
      cols.size == descending.size,
      "Length of provided list columns and their sorting direction is not equal."
    )

    val exprs = cols.zip(descending).map { case (column, bool) =>
      Expression.withPtr(column_expr.sort_column_by_name(column, bool))
    }

    sort(exprs, null_last = nullLast, maintain_order = maintainOrder)
  }

  def sort(
      expr: String,
      descending: Boolean,
      nullLast: Boolean,
      maintainOrder: Boolean
  ): LazyFrame =
    sort(Seq(expr), Seq(descending), nullLast = nullLast, maintainOrder = maintainOrder)

  def sort(exprs: Seq[Expression], null_last: Boolean, maintain_order: Boolean): LazyFrame = {
    val ldfPtr =
      lazy_frame.sortFromExprs(ptr, exprs.map(_.ptr).distinct.toArray, null_last, maintain_order)

    LazyFrame.withPtr(ldfPtr)
  }

  def sort(expr: Expression, nullLast: Boolean, maintainOrder: Boolean): LazyFrame =
    sort(Seq(expr), null_last = nullLast, maintain_order = maintainOrder)

  def limit(n: Long): LazyFrame = LazyFrame.withPtr(lazy_frame.limit(ptr, n))

  def head(n: Long): LazyFrame = limit(n)

  def first(): LazyFrame = limit(1)

  def tail(n: Long): LazyFrame = LazyFrame.withPtr(lazy_frame.tail(ptr, n))

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

  def withPtr(ptr: Long) = new LazyFrame(ptr)
}
