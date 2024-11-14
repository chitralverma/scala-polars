package org.polars.scala.polars.internal.jni.io

import org.polars.scala.polars.internal.jni.Natively

object write extends Natively {

  @native def writeParquet(
      ptr: Long,
      filePath: String,
      options: String
  ): Unit

  @native def writeIPC(
      ptr: Long,
      filePath: String,
      options: String
  ): Unit

  @native def writeAvro(
      ptr: Long,
      filePath: String,
      options: String
  ): Unit

}
