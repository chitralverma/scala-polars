package org.polars.scala.polars.api

import java.util.Collections

import scala.annotation.varargs
import scala.jdk.CollectionConverters._

import org.polars.scala.polars.api.expressions.Expression
import org.polars.scala.polars.api.io.Writeable
import org.polars.scala.polars.api.types.Schema
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
      cols: Seq[String],
      descending: Seq[Boolean],
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
        cols = Seq(expr),
        descending = Seq(descending),
        nullLast = nullLast,
        maintainOrder = maintainOrder
      )
      .collect(noOptimization = true)

  def sort(exprs: Seq[Expression], null_last: Boolean, maintain_order: Boolean): DataFrame =
    toLazy.sort(exprs, null_last, maintain_order).collect(noOptimization = true)

  def sort(expr: Expression, null_last: Boolean, maintain_order: Boolean): DataFrame =
    toLazy
      .sort(Seq(expr), null_last = null_last, maintain_order = maintain_order)
      .collect(noOptimization = true)

  def limit(n: Long): DataFrame = DataFrame.withPtr(data_frame.limit(ptr, n))

  def head(n: Long): DataFrame = limit(n)

  def first(): DataFrame = limit(1)

  def tail(n: Long): DataFrame = DataFrame.withPtr(data_frame.tail(ptr, n))

  def last(): DataFrame = tail(1)

  def withColumn(name: String, expr: Expression): DataFrame =
    toLazy.withColumn(name, expr).collect(noOptimization = true)

  @varargs
  def drop(colName: String, colNames: String*): DataFrame =
    toLazy.drop(colName, colNames: _*).collect(noOptimization = true)

  def rename(oldName: String, newName: String): DataFrame =
    rename(Collections.singletonMap(oldName, newName))

  def rename(mapping: Map[String, String]): DataFrame =
    rename(mapping.asJava)

  def rename(mapping: java.util.Map[String, String]): DataFrame =
    toLazy.rename(mapping).collect(noOptimization = true)

  def toLazy: LazyFrame = LazyFrame.withPtr(data_frame.toLazy(ptr))

  def show(): Unit = data_frame.show(ptr)

  def count(): Long = data_frame.count(ptr)

  def write(): Writeable = new Writeable(ptr)

}

object DataFrame {

  private[polars] def withPtr(ptr: Long) = new DataFrame(ptr)
}
