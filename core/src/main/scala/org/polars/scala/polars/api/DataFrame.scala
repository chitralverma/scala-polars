package org.polars.scala.polars.api

import org.polars.scala.polars.internal.jni.{Natively, data_frame}

class DataFrame private (private[polars] val ptr: Long) extends Natively {

  def show(): Unit = data_frame.show(ptr)

  def count(): Long = data_frame.count(ptr)

}

object DataFrame {

  private[polars] def withPtr(ptr: Long) = new DataFrame(ptr)
}
