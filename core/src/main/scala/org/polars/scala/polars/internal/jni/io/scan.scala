package org.polars.scala.polars.internal.jni.io

import org.polars.scala.polars.internal.jni.Natively

private[polars] object scan extends Natively {

  @native def scanParquet(paths: Array[String], options: String): Long

  @native def scanIPC(paths: Array[String], options: String): Long

  @native def scanCSV(paths: Array[String], options: String): Long

  @native def scanJsonLines(paths: Array[String], options: String): Long

}
