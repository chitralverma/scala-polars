package org.polars.scala.polars.api.expressions

import org.polars.scala.polars.internal.jni.Natively

class Expression(protected[polars] val ptr: Long) extends Natively {}

object Expression {

  private[polars] def withPtr(ptr: Long) = new Expression(ptr)

}
