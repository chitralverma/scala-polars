package org.polars.scala.polars.api.expressions

import org.polars.scala.polars.functions.lit
import org.polars.scala.polars.internal.jni.expressions.column_expr

class Column private (override protected[polars] val ptr: Long) extends Expression(ptr) {

  /** Not. */
  def unary_! : Column = Column.withPtr(column_expr.not(ptr))

  /** And. */
  def &&(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.and(ptr, rightPtr))
  }

  def and(other: Any): Column = this && other

  /** And. */
  def ||(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.or(ptr, rightPtr))
  }

  def or(other: Any): Column = this || other

  /** EqualTo. */
  def ===(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.equalTo(ptr, rightPtr))
  }

  def equalTo(other: Any): Column = this === other

  /** NotEqualTo. */
  def <>(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.notEqualTo(ptr, rightPtr))
  }

  def notEqualTo(other: Any): Column = this <> other

  /** LessThan. */
  def <(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.lessThan(ptr, rightPtr))
  }

  def lessThan(other: Any): Column = this < other

  /** LessThanEqualTo. */
  def <=(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.lessThanEqualTo(ptr, rightPtr))
  }

  def lessThanEqualTo(other: Any): Column = this <= other

  /** GreaterThan. */
  def >(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.greaterThan(ptr, rightPtr))
  }

  def greaterThan(other: Any): Column = this > other

  /** GreaterThanEqualTo. */
  def >=(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.greaterThanEqualTo(ptr, rightPtr))
  }

  def greaterThanEqualTo(other: Any): Column = this >= other

  /** Is Null. */
  def isNull: Column = Column.withPtr(column_expr.isNull(ptr))

  /** Is Not Null. */
  def isNotNull: Column = Column.withPtr(column_expr.isNotNull(ptr))

  /** Is NaN. */
  def isNaN: Column = Column.withPtr(column_expr.isNaN(ptr))

  /** Is Not NaN. */
  def isNotNaN: Column = Column.withPtr(column_expr.isNotNaN(ptr))

}

object Column {

  private[polars] def withPtr(ptr: Long) = new Column(ptr)

  private[polars] def from(name: String): Column = {
    val ptr = column_expr.column(name)
    new Column(ptr)
  }

}
