package org.polars.scala.polars.internal.jni

object schema {

  @native def column(name: String): Long

}
