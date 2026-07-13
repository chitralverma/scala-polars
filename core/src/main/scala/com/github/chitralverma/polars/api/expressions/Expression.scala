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
    Column.withPtr(column_expr.isFinite(ptr))
  }

  /** Check if the values in this expression are infinite. */
  def isInfinite: Column = {
    checkClosed()
    Column.withPtr(column_expr.isInfinite(ptr))
  }

  /** Check if the expression is empty.
    *
    * This is a whole-column aggregation returning a single boolean scalar, not a per-element
    * check. Null values are not ignored.
    */
  def isEmpty: Column = {
    checkClosed()
    Column.withPtr(column_expr.isEmpty(ptr))
  }

  /** Drop null values from this expression. */
  def dropNulls(): Column = {
    checkClosed()
    Column.withPtr(column_expr.dropNulls(ptr))
  }

  /** Drop NaN values from this expression. */
  def dropNans(): Column = {
    checkClosed()
    Column.withPtr(column_expr.dropNans(ptr))
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
    Column.withPtr(column_expr.gatherEvery(ptr, n, offset))
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
    Column.withPtr(column_expr.isUnique(ptr))
  }

  /** Mask indicating duplicated values in this expression. */
  def isDuplicated: Column = {
    checkClosed()
    Column.withPtr(column_expr.isDuplicated(ptr))
  }

  /** Mask indicating the first occurrence of distinct values in this expression. */
  def isFirstDistinct: Column = {
    checkClosed()
    Column.withPtr(column_expr.isFirstDistinct(ptr))
  }

  /** Mask indicating the last occurrence of distinct values in this expression. */
  def isLastDistinct: Column = {
    checkClosed()
    Column.withPtr(column_expr.isLastDistinct(ptr))
  }

  /** Return the most frequent values in this expression. */
  def mode(): Column = {
    checkClosed()
    Column.withPtr(column_expr.mode(ptr))
  }

  /** Return the counts of unique values in this expression. */
  def uniqueCounts(): Column = {
    checkClosed()
    Column.withPtr(column_expr.uniqueCounts(ptr))
  }

  /** Reduce groups to the sum of all the values. */
  def sum(): Column = {
    checkClosed()
    Column.withPtr(column_expr.sum(ptr))
  }

  /** Reduce groups to the minimal value. */
  def min(): Column = {
    checkClosed()
    Column.withPtr(column_expr.min(ptr))
  }

  /** Reduce groups to the maximum value. */
  def max(): Column = {
    checkClosed()
    Column.withPtr(column_expr.max(ptr))
  }

  /** Reduce groups to the mean value. */
  def mean(): Column = {
    checkClosed()
    Column.withPtr(column_expr.mean(ptr))
  }

  /** Reduce groups to the median value. */
  def median(): Column = {
    checkClosed()
    Column.withPtr(column_expr.median(ptr))
  }

  /** Standard deviation of the values.
    *
    * @param ddof
    *   Delta Degrees of Freedom (default is 1, must be between 0 and 255)
    */
  def std(ddof: Int): Column = {
    checkClosed()
    require(ddof >= 0 && ddof <= 255, s"std: 'ddof' must be between 0 and 255, but got $ddof")
    Column.withPtr(column_expr.std(ptr, ddof))
  }

  /** Standard deviation of the values with ddof = 1. */
  def std(): Column = std(1)

  /** Variance of the values.
    *
    * @param ddof
    *   Delta Degrees of Freedom (default is 1, must be between 0 and 255)
    */
  def `var`(ddof: Int): Column = {
    checkClosed()
    require(ddof >= 0 && ddof <= 255, s"var: 'ddof' must be between 0 and 255, but got $ddof")
    Column.withPtr(column_expr.`var`(ptr, ddof))
  }

  /** Variance of the values with ddof = 1. */
  def `var`(): Column = `var`(1)

  /** Variance of the values.
    *
    * @param ddof
    *   Delta Degrees of Freedom (default is 1)
    */
  def variance(ddof: Int): Column = `var`(ddof)

  /** Variance of the values with ddof = 1. */
  def variance(): Column = `var`(1)

  /** Reduce groups to the product of all the values. */
  def product(): Column = {
    checkClosed()
    Column.withPtr(column_expr.product(ptr))
  }

  /** Count the non-null values. */
  def count(): Column = {
    checkClosed()
    Column.withPtr(column_expr.count(ptr))
  }

  /** Return the number of elements (including nulls). */
  def len(): Column = {
    checkClosed()
    Column.withPtr(column_expr.len(ptr))
  }

  /** Return the number of unique values. */
  def nUnique(): Column = {
    checkClosed()
    Column.withPtr(column_expr.nUnique(ptr))
  }

  /** Return the approximate number of unique values. */
  def approxNUnique(): Column = {
    checkClosed()
    Column.withPtr(column_expr.approxNUnique(ptr))
  }

  /** Return the number of null values. */
  def nullCount(): Column = {
    checkClosed()
    Column.withPtr(column_expr.nullCount(ptr))
  }

  /** Reduce groups to the first value. */
  def first(): Column = {
    checkClosed()
    Column.withPtr(column_expr.first(ptr))
  }

  /** Reduce groups to the last value. */
  def last(): Column = {
    checkClosed()
    Column.withPtr(column_expr.last(ptr))
  }

  /** Return the quantile value of the values.
    *
    * @param q
    *   quantile value (must be between 0.0 and 1.0)
    * @param method
    *   interpolation method (e.g. "nearest", "linear")
    */
  def quantile(q: Double, method: String): Column = {
    checkClosed()
    require(q >= 0.0 && q <= 1.0, s"quantile: 'q' must be between 0.0 and 1.0, but got $q")
    Column.withPtr(column_expr.quantile(ptr, q, method))
  }

  /** Return the quantile value of the values with "nearest" interpolation.
    *
    * @param q
    *   quantile value (must be between 0.0 and 1.0)
    */
  def quantile(q: Double): Column = quantile(q, "nearest")

  /** Return the index of the minimal value. */
  def argMin(): Column = {
    checkClosed()
    Column.withPtr(column_expr.argMin(ptr))
  }

  /** Return the index of the maximum value. */
  def argMax(): Column = {
    checkClosed()
    Column.withPtr(column_expr.argMax(ptr))
  }

  /** Return the indices that would sort the values.
    *
    * @param descending
    *   whether to sort descending
    * @param nullsLast
    *   whether to put nulls last
    */
  def argSort(descending: Boolean, nullsLast: Boolean): Column = {
    checkClosed()
    Column.withPtr(column_expr.argSort(ptr, descending, nullsLast))
  }

  /** Return the indices that would sort the values ascending. */
  def argSort(): Column = argSort(descending = false, nullsLast = false)

  /** Compute the skewness of the values.
    *
    * @param bias
    *   whether to correct for statistical bias
    */
  def skew(bias: Boolean): Column = {
    checkClosed()
    Column.withPtr(column_expr.skew(ptr, bias))
  }

  /** Compute the skewness of the values with bias correction. */
  def skew(): Column = skew(bias = true)

  /** Compute the kurtosis of the values.
    *
    * @param fisher
    *   whether to use Fisher's definition of kurtosis (default is true)
    * @param bias
    *   whether to correct for statistical bias (default is true)
    */
  def kurtosis(fisher: Boolean, bias: Boolean): Column = {
    checkClosed()
    Column.withPtr(column_expr.kurtosis(ptr, fisher, bias))
  }

  /** Compute the kurtosis of the values with Fisher's definition and bias correction. */
  def kurtosis(): Column = kurtosis(fisher = true, bias = true)

  /** Return whether any of the values in the boolean expression are true.
    *
    * @param ignoreNulls
    *   whether to ignore null values
    */
  def any(ignoreNulls: Boolean): Column = {
    checkClosed()
    Column.withPtr(column_expr.any(ptr, ignoreNulls))
  }

  /** Return whether any of the values in the boolean expression are true (ignoring nulls). */
  def any(): Column = any(ignoreNulls = true)

  /** Return whether all of the values in the boolean expression are true.
    *
    * @param ignoreNulls
    *   whether to ignore null values
    */
  def all(ignoreNulls: Boolean): Column = {
    checkClosed()
    Column.withPtr(column_expr.all(ptr, ignoreNulls))
  }

  /** Return whether all of the values in the boolean expression are true (ignoring nulls). */
  def all(): Column = all(ignoreNulls = true)

  /** Compute the cumulative sum of the values.
    *
    * @param reverse
    *   whether to compute the sum in reverse order
    */
  def cumSum(reverse: Boolean): Column = {
    checkClosed()
    Column.withPtr(column_expr.cumSum(ptr, reverse))
  }

  /** Compute the cumulative sum of the values. */
  def cumSum(): Column = cumSum(reverse = false)

  /** Negate the values (unary minus). */
  def neg(): Column = {
    checkClosed()
    Column.withPtr(column_expr.neg(ptr))
  }

  /** Negate the values (unary minus). */
  def unary_- : Column = neg()

  /** Raise the values to the given power.
    *
    * @param exponent
    *   exponent to raise each value to
    */
  def pow(exponent: Double): Column = {
    checkClosed()
    Column.withPtr(column_expr.pow(ptr, exponent))
  }

  /** Floor-divide the values by another expression.
    *
    * @param other
    *   divisor expression
    */
  def floorDiv(other: Expression): Column = {
    checkClosed()
    Column.withPtr(column_expr.floorDiv(ptr, other.ptr))
  }

  /** Round the values down to the nearest integer (floor). */
  def floor(): Column = {
    checkClosed()
    Column.withPtr(column_expr.floor(ptr))
  }

  /** Round the values up to the nearest integer (ceiling). */
  def ceil(): Column = {
    checkClosed()
    Column.withPtr(column_expr.ceil(ptr))
  }

  /** Round the values to the given number of decimal places.
    *
    * Rounding uses the round-half-to-even (banker's rounding) rule.
    *
    * @param decimals
    *   number of decimal places to round to (must be >= 0)
    */
  def round(decimals: Int): Column = {
    checkClosed()
    require(decimals >= 0, s"round: 'decimals' must be >= 0, but got $decimals")
    Column.withPtr(column_expr.round(ptr, decimals))
  }

  /** Round the values to the nearest whole number. */
  def round(): Column = round(0)

  /** Round the values to the given number of significant figures.
    *
    * @param digits
    *   number of significant figures to keep (must be >= 1)
    */
  def roundSigFigs(digits: Int): Column = {
    checkClosed()
    require(digits >= 1, s"roundSigFigs: 'digits' must be >= 1, but got $digits")
    Column.withPtr(column_expr.roundSigFigs(ptr, digits))
  }

  /** Truncate the values to the given number of decimal places, rounding toward zero.
    *
    * @param decimals
    *   number of decimal places to keep (must be >= 0)
    */
  def truncate(decimals: Int): Column = {
    checkClosed()
    require(decimals >= 0, s"truncate: 'decimals' must be >= 0, but got $decimals")
    Column.withPtr(column_expr.truncate(ptr, decimals))
  }

  /** Truncate the values toward zero, dropping any fractional part. */
  def truncate(): Column = truncate(0)

  /** Compute the absolute value of each value. */
  def abs(): Column = {
    checkClosed()
    Column.withPtr(column_expr.abs(ptr))
  }

  /** Clip (limit) the values to the inclusive range defined by the given bounds.
    *
    * Values below `lower` become `lower`; values above `upper` become `upper`. Null values are
    * preserved.
    *
    * @param lower
    *   lower bound expression
    * @param upper
    *   upper bound expression
    */
  def clip(lower: Expression, upper: Expression): Column = {
    checkClosed()
    Column.withPtr(column_expr.clip(ptr, lower.ptr, upper.ptr))
  }

  /** Clip (limit) the values so none fall below the given lower bound.
    *
    * Values below `lower` become `lower`; higher values are unchanged. Null values are preserved.
    *
    * @param lower
    *   lower bound expression
    */
  def clipMin(lower: Expression): Column = {
    checkClosed()
    Column.withPtr(column_expr.clipMin(ptr, lower.ptr))
  }

  /** Clip (limit) the values so none exceed the given upper bound.
    *
    * Values above `upper` become `upper`; lower values are unchanged. Null values are preserved.
    *
    * @param upper
    *   upper bound expression
    */
  def clipMax(upper: Expression): Column = {
    checkClosed()
    Column.withPtr(column_expr.clipMax(ptr, upper.ptr))
  }

  /** Return the sign of each value: -1 for negatives, 1 for positives, 0 for zero.
    *
    * Null values are preserved and NaN maps to NaN.
    */
  def sign(): Column = {
    checkClosed()
    Column.withPtr(column_expr.sign(ptr))
  }

  /** Compute the square root of each value. */
  def sqrt(): Column = {
    checkClosed()
    Column.withPtr(column_expr.sqrt(ptr))
  }

  /** Compute the cube root of each value. */
  def cbrt(): Column = {
    checkClosed()
    Column.withPtr(column_expr.cbrt(ptr))
  }

  /** Compute the exponential (e raised to the value) of each value. */
  def exp(): Column = {
    checkClosed()
    Column.withPtr(column_expr.exp(ptr))
  }

  /** Compute the logarithm of each value to the given base.
    *
    * @param base
    *   logarithm base
    */
  def log(base: Double): Column = {
    checkClosed()
    Column.withPtr(column_expr.log(ptr, base))
  }

  /** Compute the natural logarithm (base e) of each value. */
  def log(): Column = log(math.E)

  /** Compute the base-10 logarithm of each value. */
  def log10(): Column = log(10.0)

  /** Compute the natural logarithm of one plus each value (`log(1 + x)`). */
  def log1p(): Column = {
    checkClosed()
    Column.withPtr(column_expr.log1p(ptr))
  }

  /** Compute the first discrete difference between consecutive values.
    *
    * @param n
    *   number of positions to look back when computing the difference
    * @param nullBehavior
    *   how to handle nulls: "ignore" (default) keeps them, "drop" removes them before
    *   differencing
    */
  def diff(n: Long, nullBehavior: String): Column = {
    checkClosed()
    Column.withPtr(column_expr.diff(ptr, n, nullBehavior))
  }

  /** Compute the first discrete difference between consecutive values, ignoring nulls.
    *
    * @param n
    *   number of positions to look back when computing the difference
    */
  def diff(n: Long): Column = diff(n, "ignore")

  /** Compute the difference between each value and the immediately preceding one. */
  def diff(): Column = diff(1, "ignore")

  /** Compute the fractional change between each value and the one `n` positions before it.
    *
    * @param n
    *   number of positions to look back
    */
  def pctChange(n: Long): Column = {
    checkClosed()
    Column.withPtr(column_expr.pctChange(ptr, n))
  }

  /** Compute the fractional change between consecutive values. */
  def pctChange(): Column = pctChange(1)

  /** Compute the sine of each value (in radians). */
  def sin(): Column = {
    checkClosed()
    Column.withPtr(column_expr.sin(ptr))
  }

  /** Compute the cosine of each value (in radians). */
  def cos(): Column = {
    checkClosed()
    Column.withPtr(column_expr.cos(ptr))
  }

  /** Compute the tangent of each value (in radians). */
  def tan(): Column = {
    checkClosed()
    Column.withPtr(column_expr.tan(ptr))
  }

  /** Compute the cotangent of each value (in radians). */
  def cot(): Column = {
    checkClosed()
    Column.withPtr(column_expr.cot(ptr))
  }

  /** Compute the inverse sine (arcsine) of each value, returning radians. */
  def arcsin(): Column = {
    checkClosed()
    Column.withPtr(column_expr.arcsin(ptr))
  }

  /** Compute the inverse cosine (arccosine) of each value, returning radians. */
  def arccos(): Column = {
    checkClosed()
    Column.withPtr(column_expr.arccos(ptr))
  }

  /** Compute the inverse tangent (arctangent) of each value, returning radians. */
  def arctan(): Column = {
    checkClosed()
    Column.withPtr(column_expr.arctan(ptr))
  }

  /** Compute the hyperbolic sine of each value. */
  def sinh(): Column = {
    checkClosed()
    Column.withPtr(column_expr.sinh(ptr))
  }

  /** Compute the hyperbolic cosine of each value. */
  def cosh(): Column = {
    checkClosed()
    Column.withPtr(column_expr.cosh(ptr))
  }

  /** Compute the hyperbolic tangent of each value. */
  def tanh(): Column = {
    checkClosed()
    Column.withPtr(column_expr.tanh(ptr))
  }

  /** Compute the inverse hyperbolic sine of each value. */
  def arcsinh(): Column = {
    checkClosed()
    Column.withPtr(column_expr.arcsinh(ptr))
  }

  /** Compute the inverse hyperbolic cosine of each value. */
  def arccosh(): Column = {
    checkClosed()
    Column.withPtr(column_expr.arccosh(ptr))
  }

  /** Compute the inverse hyperbolic tangent of each value. */
  def arctanh(): Column = {
    checkClosed()
    Column.withPtr(column_expr.arctanh(ptr))
  }

  /** Convert each value from radians to degrees. */
  def degrees(): Column = {
    checkClosed()
    Column.withPtr(column_expr.degrees(ptr))
  }

  /** Convert each value from degrees to radians. */
  def radians(): Column = {
    checkClosed()
    Column.withPtr(column_expr.radians(ptr))
  }

  /** Cast the values to their underlying physical representation.
    *
    * This exposes the physical storage type of logical dtypes (for example, the integer codes
    * backing a categorical, or the primitive backing a temporal type).
    */
  def toPhysical(): Column = {
    checkClosed()
    Column.withPtr(column_expr.toPhysical(ptr))
  }

  /** Reinterpret the bits of an integer column as another integer type of the same width.
    *
    * @param signed
    *   whether to reinterpret as a signed integer; `false` reinterprets as unsigned
    */
  def reinterpret(signed: Boolean): Column = {
    checkClosed()
    Column.withPtr(column_expr.reinterpret(ptr, signed))
  }

  /** Reinterpret the bits of an integer column as a signed integer of the same width. */
  def reinterpret(): Column = reinterpret(signed = true)

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
