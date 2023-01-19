package org.polars.scala.polars.internal.jni.expressions

object column_expr {

  @native def column(name: String): Long

  @native def not(ptr: Long): Long

  @native def and(leftPtr: Long, rightPtr: Long): Long

  @native def or(leftPtr: Long, rightPtr: Long): Long

  @native def equalTo(leftPtr: Long, rightPtr: Long): Long

  @native def notEqualTo(leftPtr: Long, rightPtr: Long): Long

  @native def lessThan(leftPtr: Long, rightPtr: Long): Long

  @native def greaterThan(leftPtr: Long, rightPtr: Long): Long

  @native def lessThanEqualTo(leftPtr: Long, rightPtr: Long): Long

  @native def greaterThanEqualTo(leftPtr: Long, rightPtr: Long): Long

  @native def isNull(ptr: Long): Long

  @native def isNotNull(ptr: Long): Long

  @native def isNaN(ptr: Long): Long

  @native def isNotNaN(ptr: Long): Long

}
