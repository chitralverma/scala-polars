package org.polars.scala.polars

import org.polars.scala.polars.api.io.{Readable, Scannable}
import org.polars.scala.polars.internal.jni.Natively

object Polars extends Natively {

  def scan: Scannable = new Scannable

  def read: Readable = new Readable

  @native def version(): String

}
