package org.polars.scala.polars.api.expressions

import org.polars.scala.polars.functions.lit
import org.polars.scala.polars.internal.jni.expressions.column_expr

object UnaryOperators extends Enumeration {
  type UnaryOperator = Value

  val NOT, IS_NULL, IS_NOT_NULL, IS_NAN, IS_NOT_NAN, BETWEEN, IS_IN, LIKE, CAST = Value
}

object BinaryOperators extends Enumeration {
  type BinaryOperator = Value

  val EQUAL_TO, NOT_EQUAL_TO, LESS_THAN, LESS_THAN_EQUAL_TO, GREATER_THAN, GREATER_THAN_EQUAL_TO,
      OR, AND, PLUS, MINUS, MULTIPLY, DIVIDE, MODULUS = Value
}

class Column private (override protected[polars] val ptr: Long) extends Expression(ptr) {
  import BinaryOperators._
  import UnaryOperators._

  /** Not. */
  def unary_! : Column = Column.withPtr(column_expr.applyUnary(ptr, NOT.id))

  /** Is Null. */
  def isNull: Column = Column.withPtr(column_expr.applyUnary(ptr, IS_NULL.id))

  /** Is Not Null. */
  def isNotNull: Column = Column.withPtr(column_expr.applyUnary(ptr, IS_NOT_NULL.id))

  /** Is NaN. */
  def isNaN: Column = Column.withPtr(column_expr.applyUnary(ptr, IS_NAN.id))

  /** Is Not NaN. */
  def isNotNaN: Column = Column.withPtr(column_expr.applyUnary(ptr, IS_NOT_NAN.id))

  /** Plus. */
  def +(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.applyBinary(ptr, rightPtr, PLUS.id))
  }

  def plus(other: Any): Column = this && other

  /** Minus. */
  def -(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.applyBinary(ptr, rightPtr, MINUS.id))
  }

  def minus(other: Any): Column = this && other

  /** Divide. */
  def *(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.applyBinary(ptr, rightPtr, MULTIPLY.id))
  }

  def multiply(other: Any): Column = this && other

  /** Divide. */
  def /(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.applyBinary(ptr, rightPtr, DIVIDE.id))
  }

  def divide(other: Any): Column = this && other

  /** Modulus. */
  def %(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.applyBinary(ptr, rightPtr, MODULUS.id))
  }

  def mod(other: Any): Column = this && other

  /** And. */
  def &&(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.applyBinary(ptr, rightPtr, AND.id))
  }

  def and(other: Any): Column = this && other

  /** And. */
  def ||(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.applyBinary(ptr, rightPtr, OR.id))
  }

  def or(other: Any): Column = this || other

  /** EqualTo. */
  def ===(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.applyBinary(ptr, rightPtr, EQUAL_TO.id))
  }

  def equalTo(other: Any): Column = this === other

  /** NotEqualTo. */
  def <>(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.applyBinary(ptr, rightPtr, NOT_EQUAL_TO.id))
  }

  def notEqualTo(other: Any): Column = this <> other

  /** LessThan. */
  def <(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.applyBinary(ptr, rightPtr, LESS_THAN.id))
  }

  def lessThan(other: Any): Column = this < other

  /** LessThanEqualTo. */
  def <=(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.applyBinary(ptr, rightPtr, LESS_THAN_EQUAL_TO.id))

  }

  def lessThanEqualTo(other: Any): Column = this <= other

  /** GreaterThan. */
  def >(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.applyBinary(ptr, rightPtr, GREATER_THAN.id))

  }

  def greaterThan(other: Any): Column = this > other

  /** GreaterThanEqualTo. */
  def >=(value: Any): Column = {
    val rightPtr = lit(value).ptr

    Column.withPtr(column_expr.applyBinary(ptr, rightPtr, GREATER_THAN_EQUAL_TO.id))
  }

  def greaterThanEqualTo(other: Any): Column = this >= other

}

object Column {

  private[polars] def withPtr(ptr: Long) = new Column(ptr)

  private[polars] def from(name: String): Column = {
    val ptr = column_expr.column(name)
    new Column(ptr)
  }

}
