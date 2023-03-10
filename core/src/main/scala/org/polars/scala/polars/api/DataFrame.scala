package org.polars.scala.polars.api

import org.polars.scala.polars.api.expressions.Expression
import org.polars.scala.polars.api.io.Writeable
import org.polars.scala.polars.api.types.Schema
import org.polars.scala.polars.internal.jni.data_frame

class DataFrame private (private[polars] val ptr: Long) {

  val schema: Schema = {
    val schemaString = data_frame.schemaString(ptr)
    Schema.from(schemaString)
  }

  def select(colName: String, colNames: String*): DataFrame = {
    val dfPtr = data_frame.selectFromStrings(ptr, colNames.+:(colName).distinct.toArray)

    DataFrame.withPtr(dfPtr)
  }

  def select(column: Expression, columns: Expression*): DataFrame = {
    val ldfPtr = data_frame.selectFromExprs(ptr, columns.+:(column).map(_.ptr).distinct.toArray)

    DataFrame.withPtr(ldfPtr)
  }

  def filter(predicate: Expression): DataFrame = {
    val ldfPtr = data_frame.filterFromExprs(ptr, predicate.ptr)

    DataFrame.withPtr(ldfPtr)
  }

  def toLazy(): LazyFrame = LazyFrame.withPtr(data_frame.toLazy(ptr))

  def show(): Unit = data_frame.show(ptr)

  def count(): Long = data_frame.count(ptr)
  def write(): Writeable = new Writeable(ptr)

}

object DataFrame {

  private[polars] def withPtr(ptr: Long) = new DataFrame(ptr)
}
