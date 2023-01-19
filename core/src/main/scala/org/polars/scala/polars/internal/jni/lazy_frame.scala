package org.polars.scala.polars.internal.jni

object lazy_frame extends Natively {

  @native def selectFromStrings(ptr: Long, cols: Array[String]): Long

  @native def selectFromExprs(ptr: Long, exprs: Array[Long]): Long

  @native def filterFromExprs(ldfPtr: Long, exprPtr: Long): Long

  @native def collect(ptr: Long): Long

}
