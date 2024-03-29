package org.polars.scala.polars.internal.jni.io

import org.polars.scala.polars.internal.jni.Natively

private[polars] object write extends Natively {

  @native def writeParquet(
      ptr: Long,
      filePath: String,
      writeStats: Boolean,
      compression: String,
      compressionLevel: Int,
      options: String,
      writeMode: String
  ): Unit

  @native def writeIPC(
      ptr: Long,
      filePath: String,
      compression: String,
      options: String,
      writeMode: String
  ): Unit

  @native def writeAvro(
      ptr: Long,
      filePath: String,
      compression: String,
      options: String,
      writeMode: String
  ): Unit

}
