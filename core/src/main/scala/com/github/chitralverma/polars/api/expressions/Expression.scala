package com.github.chitralverma.polars.api.expressions

import com.github.chitralverma.polars.api.types.DataType
import com.github.chitralverma.polars.internal.jni.expressions.column_expr

class Expression(protected[polars] val _ptr: Long) extends AutoCloseable {

  private var isClosed = false

  private[polars] def ptr: Long = {
    checkClosed()
    _ptr
  }

  /** Returns the namespace accessor for string-related expressions. */
  def str: ColumnStrNameSpace = {
    checkClosed()
    new ColumnStrNameSpace(this)
  }

  /** Returns the namespace accessor for expression name manipulation. */
  def name: ColumnNameNameSpace = {
    checkClosed()
    new ColumnNameNameSpace(this)
  }

  /** Rename the expression.
    *
    * @param name
    *   new name for the expression
    */
  def alias(name: String): Column = {
    checkClosed()
    Column.withPtr(column_expr.alias(ptr, name))
  }

  /** Alias for [[alias]].
    *
    * @param name
    *   new name for the expression
    */
  def as(name: String): Column = alias(name)

  /** Cast the expression to the specified DataType.
    *
    * @param dataType
    *   target data type
    */
  def cast(dataType: DataType): Column = {
    checkClosed()
    Column.withPtr(column_expr.cast(ptr, dataType.ffiName))
  }

  /** Check if the expression values are present in the provided array.
    *
    * @param values
    *   array of values to match
    */
  def isIn(values: Array[Any]): Column = {
    checkClosed()
    Column.withPtr(column_expr.isIn(ptr, values))
  }

  /** Check if the expression values are between the lower and upper bounds (inclusive).
    *
    * @param lower
    *   lower bound
    * @param upper
    *   upper bound
    */
  def isBetween(lower: Any, upper: Any): Column = {
    checkClosed()
    Column.withPtr(column_expr.isBetween(ptr, lower, upper))
  }

  /** Check if the string expression matches the given SQL-like wildcard pattern.
    *
    * @param pattern
    *   SQL-like pattern (e.g. "ap%")
    */
  def like(pattern: String): Column = {
    checkClosed()
    Column.withPtr(column_expr.like(ptr, pattern))
  }

  override def close(): Unit = synchronized {
    if (!isClosed && _ptr != 0) {
      column_expr.free(_ptr)
      isClosed = true
    }
  }

  override def finalize(): Unit = close()

  private[polars] def checkClosed(): Unit =
    if (isClosed) throw new IllegalStateException("Expression is already closed.")

}

object Expression {

  private[polars] def withPtr(ptr: Long) = new Expression(ptr)

}
