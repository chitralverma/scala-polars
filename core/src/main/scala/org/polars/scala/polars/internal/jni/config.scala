package org.polars.scala.polars.internal.jni

private[polars] object config extends Natively {

  @native def _setConfigs(options: java.util.Map[String, String]): Boolean

}
