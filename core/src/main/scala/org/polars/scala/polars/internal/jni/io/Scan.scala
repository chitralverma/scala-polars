package org.polars.scala.polars.internal.jni.io

private[polars] object Scan {

  @native def _scanParquet(filePath: String): Unit

}
