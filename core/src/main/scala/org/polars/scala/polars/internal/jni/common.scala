package org.polars.scala.polars.internal.jni

private[polars] object common extends Natively {

  @native def version(): String

  @native def setConfigs(options: java.util.Map[String, String]): Boolean

}
