package com.github.chitralverma.polars.internal.jni.expressions

import com.github.chitralverma.polars.internal.jni.Natively

private[polars] object column_expr extends Natively {

  @native def column(name: String): Long

  @native def sortColumnByName(name: String, descending: Boolean): Long

  @native def applyUnary(ptr: Long, op: Int): Long

  @native def applyBinary(leftPtr: Long, rightPtr: Long, op: Int): Long

  @native def cast(ptr: Long, dataType: String): Long

  @native def isIn(ptr: Long, values: Array[Any]): Long

  @native def isBetween(ptr: Long, lower: Any, upper: Any): Long

  @native def like(ptr: Long, pattern: String): Long

  @native def toUppercase(ptr: Long): Long

  @native def alias(ptr: Long, name: String): Long

  @native def isFinite(ptr: Long): Long

  @native def isInfinite(ptr: Long): Long

  @native def isEmpty(ptr: Long): Long

  @native def dropNulls(ptr: Long): Long

  @native def dropNans(ptr: Long): Long

  @native def reverse(ptr: Long): Long

  @native def slice(ptr: Long, offset: Long, length: Long): Long

  @native def shift(ptr: Long, periods: Long): Long

  @native def gatherEvery(ptr: Long, n: Long, offset: Long): Long

  @native def unique(ptr: Long, maintainOrder: Boolean): Long

  @native def isUnique(ptr: Long): Long

  @native def isDuplicated(ptr: Long): Long

  @native def isFirstDistinct(ptr: Long): Long

  @native def isLastDistinct(ptr: Long): Long

  @native def mode(ptr: Long): Long

  @native def uniqueCounts(ptr: Long): Long

  @native def sum(ptr: Long): Long

  @native def min(ptr: Long): Long

  @native def max(ptr: Long): Long

  @native def mean(ptr: Long): Long

  @native def median(ptr: Long): Long

  @native def std(ptr: Long, ddof: Int): Long

  @native def `var`(ptr: Long, ddof: Int): Long

  @native def product(ptr: Long): Long

  @native def count(ptr: Long): Long

  @native def len(ptr: Long): Long

  @native def nUnique(ptr: Long): Long

  @native def approxNUnique(ptr: Long): Long

  @native def nullCount(ptr: Long): Long

  @native def first(ptr: Long): Long

  @native def last(ptr: Long): Long

  @native def quantile(ptr: Long, q: Double, method: String): Long

  @native def argMin(ptr: Long): Long

  @native def argMax(ptr: Long): Long

  @native def argSort(ptr: Long, descending: Boolean, nullsLast: Boolean): Long

  @native def skew(ptr: Long, bias: Boolean): Long

  @native def kurtosis(ptr: Long, fisher: Boolean, bias: Boolean): Long

  @native def any(ptr: Long, ignoreNulls: Boolean): Long

  @native def all(ptr: Long, ignoreNulls: Boolean): Long

  @native def cumSum(ptr: Long, reverse: Boolean): Long

  @native def neg(ptr: Long): Long

  @native def pow(ptr: Long, exponent: Double): Long

  @native def floorDiv(ptr: Long, other: Long): Long

  @native def floor(ptr: Long): Long

  @native def ceil(ptr: Long): Long

  @native def round(ptr: Long, decimals: Int): Long

  @native def roundSigFigs(ptr: Long, digits: Int): Long

  @native def truncate(ptr: Long, decimals: Int): Long

  @native def abs(ptr: Long): Long

  @native def clip(ptr: Long, lower: Long, upper: Long): Long

  @native def clipMin(ptr: Long, lower: Long): Long

  @native def clipMax(ptr: Long, upper: Long): Long

  @native def sign(ptr: Long): Long

  @native def sqrt(ptr: Long): Long

  @native def cbrt(ptr: Long): Long

  @native def exp(ptr: Long): Long

  @native def log(ptr: Long, base: Double): Long

  @native def log1p(ptr: Long): Long

  @native def diff(ptr: Long, n: Long, nullBehavior: String): Long

  @native def pctChange(ptr: Long, n: Long): Long

  @native def sin(ptr: Long): Long

  @native def cos(ptr: Long): Long

  @native def tan(ptr: Long): Long

  @native def cot(ptr: Long): Long

  @native def arcsin(ptr: Long): Long

  @native def arccos(ptr: Long): Long

  @native def arctan(ptr: Long): Long

  @native def sinh(ptr: Long): Long

  @native def cosh(ptr: Long): Long

  @native def tanh(ptr: Long): Long

  @native def arcsinh(ptr: Long): Long

  @native def arccosh(ptr: Long): Long

  @native def arctanh(ptr: Long): Long

  @native def degrees(ptr: Long): Long

  @native def radians(ptr: Long): Long

  @native def toPhysical(ptr: Long): Long

  @native def reinterpret(ptr: Long, signed: Boolean): Long

  @native def free(ptr: Long): Unit

}
