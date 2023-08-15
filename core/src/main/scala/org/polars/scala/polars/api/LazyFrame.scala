package org.polars.scala.polars.api

import scala.annotation.varargs

import org.polars.scala.polars.api.expressions.Expression
import org.polars.scala.polars.api.types.Schema
import org.polars.scala.polars.internal.jni.expressions.column_expr
import org.polars.scala.polars.internal.jni.lazy_frame

class LazyFrame private (private[polars] val ptr: Long) {

  val schema: Schema = {
    val schemaString = lazy_frame.schemaString(ptr)
    Schema.from(schemaString)
  }

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
      null_last: Boolean,
      maintain_order: Boolean
  ): LazyFrame = {
    assert(
      cols.size == descending.size,
      "Length of provided list columns and their sorting direction is not equal."
    )

    val exprs = cols.zip(descending).map { case (column, bool) => {
      println("sort_column_by_name  ",(column, bool))
      Expression.withPtr(column_expr.sort_column_by_name(column, bool))
    }
    }

    sort(exprs, null_last = null_last, maintain_order = maintain_order)
  }

  def sort(
      expr: String,
      descending: Boolean,
      null_last: Boolean,
      maintain_order: Boolean
  ): LazyFrame =
    sort(Seq(expr), Seq(descending), null_last = null_last, maintain_order = maintain_order)

  def sort(exprs: Seq[Expression], null_last: Boolean, maintain_order: Boolean): LazyFrame = {
    val ldfPtr =
      lazy_frame.sortFromExprs(ptr, exprs.map(_.ptr).distinct.toArray, null_last, maintain_order)

    LazyFrame.withPtr(ldfPtr)
  }

  def sort(expr: Expression, null_last: Boolean, maintain_order: Boolean): LazyFrame =
    sort(Seq(expr), null_last = null_last, maintain_order = maintain_order)

  def withColumn(name: String, expr: Expression): LazyFrame = {
    val ldfPtr = lazy_frame.withColumn(ptr, name, expr.ptr)

    LazyFrame.withPtr(ldfPtr)
  }

  def collect(): DataFrame = {
    val dfPtr = lazy_frame.collect(ptr)
    DataFrame.withPtr(dfPtr)
  }

}

object LazyFrame {

  def withPtr(ptr: Long) = new LazyFrame(ptr)
}
