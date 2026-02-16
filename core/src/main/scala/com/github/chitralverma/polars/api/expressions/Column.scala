package com.github.chitralverma.polars.api.expressions

import com.github.chitralverma.polars.functions.lit
import com.github.chitralverma.polars.internal.jni.expressions.column_expr

object UnaryOperators extends Enumeration {
  type UnaryOperator = Value

  val NOT, IS_NULL, IS_NOT_NULL, IS_NAN, IS_NOT_NAN, BETWEEN, IS_IN, LIKE, CAST = Value
}

object BinaryOperators extends Enumeration {
  type BinaryOperator = Value

  val EQUAL_TO, NOT_EQUAL_TO, LESS_THAN, LESS_THAN_EQUAL_TO, GREATER_THAN, GREATER_THAN_EQUAL_TO,
      OR, AND, PLUS, MINUS, MULTIPLY, DIVIDE, MODULUS = Value
}

class Column private (p: Long) extends Expression(p) {
  import BinaryOperators._
  import UnaryOperators._

  /** Not. */
  def unary_! : Column = {
    checkClosed()
    Column.withPtr(column_expr.applyUnary(ptr, NOT.id))
  }

  /** Is Null. */
  def isNull: Column = {
    checkClosed()
    Column.withPtr(column_expr.applyUnary(ptr, IS_NULL.id))
  }

  /** Is Not Null. */
  def isNotNull: Column = {
    checkClosed()
    Column.withPtr(column_expr.applyUnary(ptr, IS_NOT_NULL.id))
  }

  /** Is NaN. */
  def isNaN: Column = {
    checkClosed()
    Column.withPtr(column_expr.applyUnary(ptr, IS_NAN.id))
  }

  /** Is Not NaN. */
  def isNotNaN: Column = {
    checkClosed()
    Column.withPtr(column_expr.applyUnary(ptr, IS_NOT_NAN.id))
  }

  /** Plus. */
  def +(value: Any): Column = applyBinaryOp(value, PLUS.id)

  /** Minus. */
  def -(value: Any): Column = applyBinaryOp(value, MINUS.id)

  /** Divide. */
  def *(value: Any): Column = applyBinaryOp(value, MULTIPLY.id)

  /** Divide. */
  def /(value: Any): Column = applyBinaryOp(value, DIVIDE.id)

  /** Modulus. */
  def %(value: Any): Column = applyBinaryOp(value, MODULUS.id)

  /** And. */
  def &&(value: Any): Column = applyBinaryOp(value, AND.id)

  /** And. */
  def ||(value: Any): Column = applyBinaryOp(value, OR.id)

  /** EqualTo. */
  def ===(value: Any): Column = applyBinaryOp(value, EQUAL_TO.id)

  /** NotEqualTo. */
  def <>(value: Any): Column = applyBinaryOp(value, NOT_EQUAL_TO.id)

  /** LessThan. */
  def <(value: Any): Column = applyBinaryOp(value, LESS_THAN.id)

  /** LessThanEqualTo. */
  def <=(value: Any): Column = applyBinaryOp(value, LESS_THAN_EQUAL_TO.id)

  /** GreaterThan. */
  def >(value: Any): Column = applyBinaryOp(value, GREATER_THAN.id)

  /** GreaterThanEqualTo. */
  def >=(value: Any): Column = applyBinaryOp(value, GREATER_THAN_EQUAL_TO.id)

  def plus(other: Any): Column = {
    checkClosed()
    this + other
  }

  def minus(other: Any): Column = {
    checkClosed()
    this - other
  }

  def multiply(other: Any): Column = {
    checkClosed()
    this * other
  }

  def divide(other: Any): Column = {
    checkClosed()
    this / other
  }

  def mod(other: Any): Column = {
    checkClosed()
    this % other
  }

  def and(other: Any): Column = {
    checkClosed()
    this && other
  }

  def or(other: Any): Column = {
    checkClosed()
    this || other
  }

  def equalTo(other: Any): Column = {
    checkClosed()
    this === other
  }

  def notEqualTo(other: Any): Column = {
    checkClosed()
    this <> other
  }

  def lessThan(other: Any): Column = {
    checkClosed()
    this < other
  }

  def lessThanEqualTo(other: Any): Column = {
    checkClosed()
    this <= other
  }

  def greaterThan(other: Any): Column = {
    checkClosed()
    this > other
  }

  def greaterThanEqualTo(other: Any): Column = {
    checkClosed()
    this >= other
  }

  private def applyBinaryOp(value: Any, opId: Int): Column = {
    checkClosed()
    val right = lit(value)
    try
      Column.withPtr(column_expr.applyBinary(ptr, right.ptr, opId))
    finally
      // If 'right' is a newly created literal expression (not one provided by the user), close it.
      if (right ne value.asInstanceOf[AnyRef]) right.close()
  }

}

object Column {

  private[polars] def withPtr(ptr: Long) = new Column(ptr)

  private[polars] def from(name: String): Column = {
    val ptr = column_expr.column(name)
    new Column(ptr)
  }

}
