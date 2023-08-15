package org.polars.scala.polars.internal.jni.expressions

import org.polars.scala.polars.internal.jni.Natively

private[polars] object column_expr extends Natively {

  @native def column(name: String): Long

  @native def sort_column_by_name(name: String, descending: Boolean): Long

  @native def sort_expr(ptr: Long, descending: Boolean): Long

  @native def applyUnary(ptr: Long, op: Int): Long

  @native def applyBinary(leftPtr: Long, rightPtr: Long, op: Int): Long

}
