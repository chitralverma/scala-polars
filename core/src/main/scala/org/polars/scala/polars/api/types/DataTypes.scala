package org.polars.scala.polars.api.types

import java.time.ZoneId
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.util.matching.Regex

trait DataType {
  def simpleName: String =
    this.getClass.getSimpleName
      .stripSuffix("$")
      .stripSuffix("Type")
      .stripSuffix("UDT")
      .toLowerCase(Locale.ROOT)
}

trait BasicDataType extends DataType

case object StringType extends BasicDataType

case object BooleanType extends BasicDataType

case object IntType extends BasicDataType

case object LongType extends BasicDataType

case object FloatType extends BasicDataType

case object DoubleType extends BasicDataType

case object DateType extends BasicDataType

case class DateTimeType(precision: TimeUnit, timezone: ZoneId) extends DataType

case class ListType(tpe: DataType) extends DataType {
  override def simpleName: String = "list"

  /** Borrowed from Apache Spark source to represent [[ListType]] as a tree string. */
  private[polars] def buildFormattedString(prefix: String, buffer: StringBuffer): Unit = {
    buffer.append(s"$prefix-- element: ${tpe.simpleName}\n")
    DataType.buildFormattedString(tpe, s"$prefix    |", buffer)
  }

}

case class StructType(fields: Seq[Field]) extends DataType {
  override def simpleName: String = "struct"

  /** Borrowed from Apache Spark source to represent [[StructType]] as a tree string. */
  private[polars] def buildFormattedString(prefix: String, buffer: StringBuffer): Unit =
    fields.foreach(field => field.buildFormattedString(prefix, buffer))
}

object DataType {

  private[polars] final val StringRegex: Regex = """^(?i)Utf8|LargeUtf8|String$""".r
  private[polars] final val BooleanRegex: Regex = """^(?i)Boolean$""".r
  private[polars] final val IntRegex: Regex = """^(?i)Int8|Int16|Int32|UInt8|UInt16|UInt32$""".r
  private[polars] final val LongRegex: Regex = """^(?i)Int64|UInt64$""".r
  private[polars] final val FloatRegex: Regex = """^(?i)Float32$""".r
  private[polars] final val DoubleRegex: Regex = """^(?i)Float64$""".r
  private[polars] final val DateRegex: Regex = """^(?i)Date$""".r

  def fromBasicType(typeStr: String): DataType = typeStr match {
    case StringRegex() => StringType
    case BooleanRegex() => BooleanType
    case IntRegex() => IntType
    case LongRegex() => LongType
    case FloatRegex() => FloatType
    case DoubleRegex() => DoubleType
    case DateRegex() => DateType
    case typeStr =>
      throw new IllegalArgumentException(s"Unknown basic type `$typeStr` is not supported.")
  }

  /** Borrowed from Apache Spark source to represent [[DataType]] as a tree string. */
  private[polars] def buildFormattedString(
      dataType: DataType,
      prefix: String,
      buffer: StringBuffer
  ): Unit =
    dataType match {
      case array: ListType => array.buildFormattedString(prefix, buffer)
      case struct: StructType => struct.buildFormattedString(prefix, buffer)
      case _ =>
    }

}
