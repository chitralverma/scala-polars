package org.polars.scala.polars.api

import org.polars.scala.polars.api.expressions.Expression
import org.polars.scala.polars.api.types.Schema
import org.polars.scala.polars.internal.jni.lazy_frame

import scala.annotation.varargs

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
