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

  @native def free(ptr: Long): Unit

}
