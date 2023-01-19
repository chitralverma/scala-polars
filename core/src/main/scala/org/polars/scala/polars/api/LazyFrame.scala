package org.polars.scala.polars.api

import org.polars.scala.polars.api.expressions.Expression
import org.polars.scala.polars.internal.jni.{Natively, lazy_frame}

class LazyFrame private (private[polars] val ptr: Long) extends Natively {

  def select(colName: String, colNames: String*): LazyFrame = {
    val ldfPtr = lazy_frame.selectFromStrings(ptr, colNames.+:(colName).distinct.toArray)

    LazyFrame.withPtr(ldfPtr)
  }

  def select(column: Expression, columns: Expression*): LazyFrame = {
    val ldfPtr = lazy_frame.selectFromExprs(ptr, columns.+:(column).map(_.ptr).distinct.toArray)

    LazyFrame.withPtr(ldfPtr)
  }

  def collect(): DataFrame = {
    val dfPtr = lazy_frame.collect(ptr)
    DataFrame.withPtr(dfPtr)
  }

}

object LazyFrame {

  private[polars] def withPtr(ptr: Long) = new LazyFrame(ptr)
}
