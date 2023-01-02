package org.polars.scala.polars

import org.polars.scala.polars.api.io.{Readable, Scanable}
import org.polars.scala.polars.internal.jni.Natively

object Polars extends Natively {

  def scan: Scanable = new Scanable

  def read: Readable = new Readable

  @native def version(): String

}
