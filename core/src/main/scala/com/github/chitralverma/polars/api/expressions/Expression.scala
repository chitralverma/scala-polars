package com.github.chitralverma.polars.api.expressions

class Expression(protected[polars] val ptr: Long) {}

object Expression {

  private[polars] def withPtr(ptr: Long) = new Expression(ptr)

}
