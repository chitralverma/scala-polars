package org.polars.scala.polars.internal.jni.expressions

import java.sql.{Date, Timestamp}

object literal_expr {

  @native def nullLit(): Long

  @native def fromString(value: String): Long

  @native def fromBool(value: Boolean): Long

  @native def fromInt(value: Int): Long

  @native def fromLong(value: Long): Long

  @native def fromFloat(value: Float): Long

  @native def fromDouble(value: Double): Long

  @native def fromDate(value: String): Long

  @native def fromTimestamp(value: String): Long

}
