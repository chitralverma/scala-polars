package com.github.chitralverma.polars.internal.jni.expressions

import com.github.chitralverma.polars.internal.jni.Natively

private[polars] object functions_expr extends Natively {

  @native def anyHorizontal(ptrs: Array[Long]): Long

  @native def allHorizontal(ptrs: Array[Long]): Long

  @native def maxHorizontal(ptrs: Array[Long]): Long

  @native def minHorizontal(ptrs: Array[Long]): Long

  @native def sumHorizontal(ptrs: Array[Long], ignoreNulls: Boolean): Long

  @native def meanHorizontal(ptrs: Array[Long], ignoreNulls: Boolean): Long

  @native def ternaryExpr(predicate: Long, truthy: Long, falsy: Long): Long

  @native def arctan2(y: Long, x: Long): Long

}
