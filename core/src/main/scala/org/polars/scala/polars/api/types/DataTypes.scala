package org.polars.scala.polars.api.types

import java.time.ZoneId
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.util.Try
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

case class TimeType(protected val unitStr: String) extends DataType {
  val timeUnit: Option[TimeUnit] =
    unitStr match {
      case s if s.toLowerCase(Locale.ROOT).contains("nano") => Some(TimeUnit.NANOSECONDS)
      case s if s.toLowerCase(Locale.ROOT).contains("micro") => Some(TimeUnit.MICROSECONDS)
      case s if s.toLowerCase(Locale.ROOT).contains("milli") => Some(TimeUnit.MILLISECONDS)
      case _ => None
    }

  override def simpleName: String = timeUnit match {
    case Some(TimeUnit.NANOSECONDS) => "time[ns]"
    case Some(TimeUnit.MICROSECONDS) => "time[us]"
    case Some(TimeUnit.MILLISECONDS) => "time[ms]"
    case _ => "time"
  }
}

case class DateTimeType(protected val unitStr: String, protected val tzStr: String)
    extends DataType {
  val timeUnit: Option[TimeUnit] =
    unitStr match {
      case null => None
      case s if s.toLowerCase(Locale.ROOT).contains("nano") => Some(TimeUnit.NANOSECONDS)
      case s if s.toLowerCase(Locale.ROOT).contains("micro") => Some(TimeUnit.MICROSECONDS)
      case s if s.toLowerCase(Locale.ROOT).contains("milli") => Some(TimeUnit.MILLISECONDS)
      case _ => None
    }

  val timeZone: Option[ZoneId] = Try(ZoneId.of(tzStr)).toOption

  override def simpleName: String = {
    val tu = timeUnit match {
      case Some(TimeUnit.NANOSECONDS) => "ns"
      case Some(TimeUnit.MICROSECONDS) => "us"
      case Some(TimeUnit.MILLISECONDS) => "ms"
      case _ => null
    }

    val tz = timeZone.orNull

    (tu, tz) match {
      case (null, null) => "datetime"
      case (tu, null) => s"datetime[$tu]"
      case (null, tz) => s"datetime[$tz]"
      case (tu, tz) => s"datetime[$tu, $tz]"
    }

  }
}

case class Duration(protected val unitStr: String) extends DataType {
  val timeUnit: Option[TimeUnit] =
    unitStr match {
      case s if s.toLowerCase(Locale.ROOT).contains("nano") => Some(TimeUnit.NANOSECONDS)
      case s if s.toLowerCase(Locale.ROOT).contains("micro") => Some(TimeUnit.MICROSECONDS)
      case s if s.toLowerCase(Locale.ROOT).contains("milli") => Some(TimeUnit.MILLISECONDS)
      case _ => None
    }

  override def simpleName: String = timeUnit match {
    case Some(TimeUnit.NANOSECONDS) => "duration[ns]"
    case Some(TimeUnit.MICROSECONDS) => "duration[us]"
    case Some(TimeUnit.MILLISECONDS) => "duration[ms]"
    case _ => "duration"
  }
}

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
  private[polars] final val DateRegex: Regex = """^(?i)Date|Date32|Date64$""".r

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
