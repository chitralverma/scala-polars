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
  def isFinite: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_finite(ptr))
  }

  /** Check if the values in this expression are infinite. */
  def isInfinite: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_infinite(ptr))
  }

  /** Check if the expression is empty.
    *
    * This is a whole-column aggregation returning a single boolean scalar, not a per-element
    * check. Null values are not ignored.
    */
  def isEmpty: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_empty(ptr))
  }

  /** Drop null values from this expression. */
  def dropNulls(): Column = {
    checkClosed()
    Column.withPtr(column_expr.drop_nulls(ptr))
  }

  /** Drop NaN values from this expression. */
  def dropNans(): Column = {
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
    *   number of elements to return
    */
  def head(n: Long): Column = {
    checkClosed()
    slice(0, n)
  }

  /** Get the first 10 elements of this expression. */
  def head(): Column = head(10)

  /** Get the last n elements of this expression.
    *
    * @param n
    *   number of elements to return
    */
  def tail(n: Long): Column = {
    checkClosed()
    slice(-n, n)
  }

  /** Get the last 10 elements of this expression. */
  def tail(): Column = tail(10)

  /** Alias for `head`.
    *
    * @param n
    *   number of elements to return
    */
  def limit(n: Long): Column = head(n)

  /** Alias for `head`. */
  def limit(): Column = limit(10)

  /** Gather every nth element, starting at the offset.
    *
    * @param n
    *   gather elements with this step size (must be >= 1)
    * @param offset
    *   start gathering from this index (default is 0, must be >= 0)
    */
  def gatherEvery(n: Long, offset: Long): Column = {
    checkClosed()
    require(n >= 1, s"gatherEvery: step size 'n' must be >= 1, but got $n")
    require(offset >= 0, s"gatherEvery: 'offset' must be >= 0, but got $offset")
    Column.withPtr(column_expr.gather_every(ptr, n, offset))
  }

  /** Gather every nth element.
    *
    * @param n
    *   gather elements with this step size (must be >= 1)
    */
  def gatherEvery(n: Long): Column = gatherEvery(n, 0)

  /** Shift elements in this expression by periods.
    *
    * @param periods
    *   number of positions to shift
    */
  def shift(periods: Long): Column = {
    checkClosed()
    Column.withPtr(column_expr.shift(ptr, periods))
  }

  /** Shift elements in this expression by 1 position. */
  def shift(): Column = shift(1)

  /** Get unique values from this expression.
    *
    * @param maintainOrder
    *   whether to maintain the original order of elements
    */
  def unique(maintainOrder: Boolean): Column = {
    checkClosed()
    Column.withPtr(column_expr.unique(ptr, maintainOrder))
  }

  /** Get unique values from this expression. */
  def unique(): Column = unique(false)

  /** Mask indicating unique values in this expression. */
  def isUnique: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_unique(ptr))
  }

  /** Mask indicating duplicated values in this expression. */
  def isDuplicated: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_duplicated(ptr))
  }

  /** Mask indicating the first occurrence of distinct values in this expression. */
  def isFirstDistinct: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_first_distinct(ptr))
  }

  /** Mask indicating the last occurrence of distinct values in this expression. */
  def isLastDistinct: Column = {
    checkClosed()
    Column.withPtr(column_expr.is_last_distinct(ptr))
  }

  /** Return the most frequent values in this expression. */
  def mode(): Column = {
    checkClosed()
    Column.withPtr(column_expr.mode(ptr))
  }

  /** Return the counts of unique values in this expression. */
  def uniqueCounts(): Column = {
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
