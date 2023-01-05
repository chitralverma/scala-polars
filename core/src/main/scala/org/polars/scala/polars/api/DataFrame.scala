package org.polars.scala.polars.api

import org.polars.scala.polars.internal.jni.{Natively, data_frame}

class DataFrame private (ptrLong: Long) extends Natively {

  def show(): Unit = data_frame.show(ptrLong)

  def count(): Long = data_frame.count(ptrLong)

}

object DataFrame {

  private[polars] def withPtr(ptr: Long) = new DataFrame(ptr)
}
