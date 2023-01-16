package org.polars.scala.polars.api

import org.polars.scala.polars.internal.jni.schema.column

class Column private (private[polars] val ptr: Long) extends Expression {}

object Column {

  private[polars] def withPtr(ptr: Long) = new Column(ptr)

  def from(name: String): Column = {
    val ptr = column(name)
    new Column(ptr)
  }

}
