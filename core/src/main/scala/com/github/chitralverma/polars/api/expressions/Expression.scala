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

  /** Check if the values in this expression are finite. */
  def is_finite: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_finite(ptr))
  }

  /** Check if the values in this expression are infinite. */
  def is_infinite: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_infinite(ptr))
  }

  /** Check if the expression is empty.
    *
    * @note
    *   This function is unstable in Polars and may change in future versions.
    */
  def is_empty: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_empty(ptr))
  }

  /** Drop null values from this expression. */
  def drop_nulls(): Column = {
    checkClosed()
    Column.withPtr(column_expr.drop_nulls(ptr))
  }

  /** Drop NaN values from this expression. */
  def drop_nans(): Column = {
    checkClosed()
    Column.withPtr(column_expr.drop_nans(ptr))
  }

  /** Reverse the order of elements in this expression. */
  def reverse(): Column = {
    checkClosed()
    Column.withPtr(column_expr.reverse(ptr))
  }

  /** Get a slice of this expression.
    *
    * @param offset
    *   start index of the slice (negative indexing is supported)
    * @param length
    *   length of the slice
    */
  def slice(offset: Long, length: Long): Column = {
    checkClosed()
    Column.withPtr(column_expr.slice(ptr, offset, length))
  }

  /** Get the first n elements of this expression.
    *
    * @param n
    *   number of elements to return (default is 10)
    */
  def head(n: Long = 10): Column = {
    checkClosed()
    slice(0, n)
  }

  /** Get the last n elements of this expression.
    *
    * @param n
    *   number of elements to return (default is 10)
    */
  def tail(n: Long = 10): Column = {
    checkClosed()
    slice(-n, n)
  }

  /** Alias for [[head]].
    *
    * @param n
    *   number of elements to return (default is 10)
    */
  def limit(n: Long = 10): Column = head(n)

  /** Gather every nth element, starting at the offset.
    *
    * @param n
    *   gather elements with this step size
    * @param offset
    *   start gathering from this index (default is 0)
    */
  def gather_every(n: Long, offset: Long = 0): Column = {
    checkClosed()
    Column.withPtr(column_expr.gather_every(ptr, n, offset))
  }

  /** Shift elements in this expression by periods.
    *
    * @param periods
    *   number of positions to shift
    */
  def shift(periods: Long = 1): Column = {
    checkClosed()
    Column.withPtr(column_expr.shift(ptr, periods))
  }

  /** Get unique values from this expression.
    *
    * @param maintainOrder
    *   whether to maintain the original order of elements
    */
  def unique(maintainOrder: Boolean = false): Column = {
    checkClosed()
    Column.withPtr(column_expr.unique(ptr, maintainOrder))
  }

  /** Mask indicating unique values in this expression. */
  def is_unique: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_unique(ptr))
  }

  /** Mask indicating duplicated values in this expression. */
  def is_duplicated: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_duplicated(ptr))
  }

  /** Mask indicating the first occurrence of distinct values in this expression. */
  def is_first_distinct: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_first_distinct(ptr))
  }

  /** Mask indicating the last occurrence of distinct values in this expression. */
  def is_last_distinct: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_last_distinct(ptr))
  }

  /** Return the most frequent values in this expression. */
  def mode(): Column = {
    checkClosed()
    Column.withPtr(column_expr.mode(ptr))
  }

  /** Return the counts of unique values in this expression. */
  def unique_counts(): Column = {
    checkClosed()
    Column.withPtr(column_expr.unique_counts(ptr))
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
