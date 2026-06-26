package com.github.chitralverma.polars.internal.jni.expressions

import com.github.chitralverma.polars.internal.jni.Natively

private[polars] object column_expr extends Natively {

  @native def column(name: String): Long

  @native def sort_column_by_name(name: String, descending: Boolean): Long

  @native def applyUnary(ptr: Long, op: Int): Long

  @native def applyBinary(leftPtr: Long, rightPtr: Long, op: Int): Long

  @native def cast(ptr: Long, dataType: String): Long

  @native def isIn(ptr: Long, values: Array[Any]): Long

  @native def isBetween(ptr: Long, lower: Any, upper: Any): Long

  @native def like(ptr: Long, pattern: String): Long

  @native def to_uppercase(ptr: Long): Long

  @native def alias(ptr: Long, name: String): Long

  @native def is_finite(ptr: Long): Long

  @native def is_infinite(ptr: Long): Long

  @native def is_empty(ptr: Long): Long

  @native def drop_nulls(ptr: Long): Long

  @native def drop_nans(ptr: Long): Long

  @native def reverse(ptr: Long): Long

  @native def slice(ptr: Long, offset: Long, length: Long): Long

  @native def shift(ptr: Long, periods: Long): Long

  @native def gather_every(ptr: Long, n: Long, offset: Long): Long

  @native def unique(ptr: Long, maintainOrder: Boolean): Long

  @native def is_unique(ptr: Long): Long

  @native def is_duplicated(ptr: Long): Long

  @native def is_first_distinct(ptr: Long): Long

  @native def is_last_distinct(ptr: Long): Long

  @native def mode(ptr: Long): Long

  @native def unique_counts(ptr: Long): Long

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

  @native def n_unique(ptr: Long): Long

  @native def approx_n_unique(ptr: Long): Long

  @native def null_count(ptr: Long): Long

  @native def first(ptr: Long): Long

  @native def last(ptr: Long): Long

  @native def quantile(ptr: Long, q: Double, method: String): Long

  @native def arg_min(ptr: Long): Long

  @native def arg_max(ptr: Long): Long

  @native def arg_sort(ptr: Long, descending: Boolean, nullsLast: Boolean): Long

  @native def skew(ptr: Long, bias: Boolean): Long

  @native def kurtosis(ptr: Long, fisher: Boolean, bias: Boolean): Long

  @native def free(ptr: Long): Unit

}
