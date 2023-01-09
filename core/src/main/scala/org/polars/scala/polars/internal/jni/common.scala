package org.polars.scala.polars.internal.jni

private[polars] object common extends Natively {

  @native def _version(): String

  @native def _concatLazyFrames(ptrs: Array[Long], reChunk: Boolean, parallel: Boolean): Long

  @native def _concatDataFrames(ptrs: Array[Long]): Long

}
