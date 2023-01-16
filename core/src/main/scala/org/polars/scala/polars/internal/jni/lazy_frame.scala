package org.polars.scala.polars.internal.jni

object lazy_frame extends Natively {

  @native def select(ptr: Long, exprs: Array[String]): Long

  @native def collect(ptr: Long): Long

}
