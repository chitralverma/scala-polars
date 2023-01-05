package org.polars.scala.polars.internal.jni

object lazy_frame extends Natively {

  @native def collect(ptr: Long): Long

}
