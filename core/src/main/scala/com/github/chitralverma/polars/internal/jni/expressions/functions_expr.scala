package com.github.chitralverma.polars.internal.jni.expressions

import com.github.chitralverma.polars.internal.jni.Natively

private[polars] object functions_expr extends Natively {

  @native def any_horizontal(ptrs: Array[Long]): Long

  @native def all_horizontal(ptrs: Array[Long]): Long

  @native def max_horizontal(ptrs: Array[Long]): Long

  @native def min_horizontal(ptrs: Array[Long]): Long

  @native def sum_horizontal(ptrs: Array[Long], ignoreNulls: Boolean): Long

  @native def mean_horizontal(ptrs: Array[Long], ignoreNulls: Boolean): Long

}
