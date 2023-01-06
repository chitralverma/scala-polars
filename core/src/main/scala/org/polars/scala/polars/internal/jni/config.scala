package org.polars.scala.polars.internal.jni

object config {

  @native def _setConfigs(options: java.util.Map[String, String]): Boolean

}
