package org.polars.scala.polars.internal.jni

object data_frame extends Natively {

  @native def show(ptr: Long): Unit

  @native def count(ptr: Long): Long

}
