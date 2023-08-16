package org.polars.scala.polars.internal.jni

private[polars] object data_frame extends Natively {

  @native def concatDataFrames(ptrs: Array[Long]): Long

  @native def schemaString(ptr: Long): String

  @native def toLazy(ptr: Long): Long

  @native def show(ptr: Long): Unit

  @native def count(ptr: Long): Long

  @native def limit(ptr: Long, n: Long): Long

}
