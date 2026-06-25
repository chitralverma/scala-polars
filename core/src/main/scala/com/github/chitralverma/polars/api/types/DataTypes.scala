package com.github.chitralverma.polars.api.types

import java.time.ZoneId
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.matching.Regex

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.node.ObjectNode
import com.github.chitralverma.polars.jsonMapper

trait DataType {

  /** Returns the simple lowercase name of this DataType. */
  def simpleName: String =
    this.getClass.getSimpleName
      .stripSuffix("$")
      .stripSuffix("Type")
      .toLowerCase(Locale.ROOT)

  /** Returns the official JSON string representation of this DataType, compatible with Polars'
    * upstream serde/FFI deserializer.
    */
  def ffiName: String = {
    val node = toJsonNode
    if (node.isTextual) node.textValue()
    else jsonMapper.writeValueAsString(node)
  }

  private[polars] def toJsonNode: JsonNode
}

trait BasicDataType extends DataType {
  override private[polars] def toJsonNode: JsonNode =
    jsonMapper.getNodeFactory.textNode(
      this.getClass.getSimpleName
        .stripSuffix("$")
        .stripSuffix("Type")
    )
}

case object StringType extends BasicDataType {
  override def simpleName: String = "string"
}

case object BooleanType extends BasicDataType {
  override def simpleName: String = "boolean"
}

case object Int8Type extends BasicDataType {
  override def simpleName: String = "int8"
}

case object Int16Type extends BasicDataType {
  override def simpleName: String = "int16"
}

case object Int32Type extends BasicDataType {
  override def simpleName: String = "int32"
}

case object Int64Type extends BasicDataType {
  override def simpleName: String = "int64"
}

case object UInt8Type extends BasicDataType {
  override def simpleName: String = "uint8"
}

case object UInt16Type extends BasicDataType {
  override def simpleName: String = "uint16"
}

case object UInt32Type extends BasicDataType {
  override def simpleName: String = "uint32"
}

case object UInt64Type extends BasicDataType {
  override def simpleName: String = "uint64"
}

case object Float32Type extends BasicDataType {
  override def simpleName: String = "float32"
}

case object Float64Type extends BasicDataType {
  override def simpleName: String = "float64"
}

case class DecimalType(precision: Option[Int], scale: Int) extends DataType {
  override def simpleName: String = s"decimal[${precision.getOrElse(38)},$scale]"
  override private[polars] def toJsonNode: JsonNode = {
    val root = jsonMapper.getNodeFactory.objectNode()
    val arr = jsonMapper.getNodeFactory.arrayNode()
    precision match {
      case Some(p) => arr.add(p)
      case None => arr.addNull()
    }
    arr.add(scale)
    root.set("Decimal", arr)
    root
  }
}

case object DateType extends BasicDataType {
  override def simpleName: String = "date"
}

case object TimeType extends DataType {
  override private[polars] def toJsonNode: JsonNode =
    jsonMapper.getNodeFactory.textNode("Time")
}

case object NullType extends BasicDataType {
  override def simpleName: String = "null"
}

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

  override private[polars] def toJsonNode: JsonNode = {
    val root = jsonMapper.getNodeFactory.objectNode()
    val u = timeUnit match {
      case Some(TimeUnit.NANOSECONDS) => "Nanoseconds"
      case Some(TimeUnit.MICROSECONDS) => "Microseconds"
      case Some(TimeUnit.MILLISECONDS) => "Milliseconds"
      case _ => "Microseconds"
    }
    root.put("Time", u)
    root
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

  override private[polars] def toJsonNode: JsonNode = {
    val root = jsonMapper.getNodeFactory.objectNode()
    val arr = jsonMapper.getNodeFactory.arrayNode()
    val tu = timeUnit match {
      case Some(TimeUnit.NANOSECONDS) => "Nanoseconds"
      case Some(TimeUnit.MICROSECONDS) => "Microseconds"
      case Some(TimeUnit.MILLISECONDS) => "Milliseconds"
      case _ => "Microseconds"
    }
    arr.add(tu)
    if (tzStr != null && tzStr.nonEmpty) arr.add(tzStr)
    else arr.addNull()
    root.set("Timestamp", arr)
    root
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

  override private[polars] def toJsonNode: JsonNode = {
    val root = jsonMapper.getNodeFactory.objectNode()
    val tu = timeUnit match {
      case Some(TimeUnit.NANOSECONDS) => "Nanoseconds"
      case Some(TimeUnit.MICROSECONDS) => "Microseconds"
      case Some(TimeUnit.MILLISECONDS) => "Milliseconds"
      case _ => "Microseconds"
    }
    root.put("Duration", tu)
    root
  }
}

case class ListType(tpe: DataType) extends DataType {
  override def simpleName: String = "list"

  /** Helper to represent [[ListType]] as a tree string. */
  private[polars] def buildFormattedString(prefix: String, buffer: StringBuffer): Unit = {
    buffer.append(s"$prefix-- element: ${tpe.simpleName}\n")
    DataType.buildFormattedString(tpe, s"$prefix    |", buffer)
  }

  override private[polars] def toJsonNode: JsonNode = {
    val root = jsonMapper.getNodeFactory.objectNode()
    val listInner = jsonMapper.getNodeFactory.objectNode()
    listInner.set("dtype", tpe.toJsonNode)
    root.set("List", listInner)
    root
  }
}

case class StructType(fields: Array[Field]) extends DataType {
  override def simpleName: String = "struct"

  def toSchema: Schema = Schema.fromFields(fields)

  /** Helper to represent [[StructType]] as a tree string. */
  private[polars] def buildFormattedString(prefix: String, buffer: StringBuffer): Unit =
    fields.foreach(field => field.buildFormattedString(prefix, buffer))

  override private[polars] def toJsonNode: JsonNode = {
    val root = jsonMapper.getNodeFactory.objectNode()
    val arr = jsonMapper.getNodeFactory.arrayNode()
    fields.foreach { f =>
      val fNode = jsonMapper.getNodeFactory.objectNode()
      fNode.put("name", f.name)
      fNode.set("dtype", f.dataType.toJsonNode)
      arr.add(fNode)
    }
    root.set("Struct", arr)
    root
  }
}

object DataType {

  private[polars] final val StringRegex: Regex = """^(?i)Utf8|LargeUtf8|String$""".r
  private[polars] final val BooleanRegex: Regex = """^(?i)Boolean$""".r
  private[polars] final val Int8Regex: Regex = """^(?i)Int8$""".r
  private[polars] final val Int16Regex: Regex = """^(?i)Int16$""".r
  private[polars] final val Int32Regex: Regex = """^(?i)Int32$""".r
  private[polars] final val Int64Regex: Regex = """^(?i)Int64$""".r
  private[polars] final val UInt8Regex: Regex = """^(?i)UInt8$""".r
  private[polars] final val UInt16Regex: Regex = """^(?i)UInt16$""".r
  private[polars] final val UInt32Regex: Regex = """^(?i)UInt32$""".r
  private[polars] final val UInt64Regex: Regex = """^(?i)UInt64$""".r
  private[polars] final val FloatRegex: Regex = """^(?i)Float32$""".r
  private[polars] final val DoubleRegex: Regex = """^(?i)Float64$""".r
  private[polars] final val DateRegex: Regex = """^(?i)Date|Date32|Date64$""".r
  private[polars] final val NullRegex: Regex = """^(?i)Null$""".r

  def fromBasicType(typeStr: String): DataType = typeStr match {
    case StringRegex() => StringType
    case BooleanRegex() => BooleanType
    case Int8Regex() => Int8Type
    case Int16Regex() => Int16Type
    case Int32Regex() => Int32Type
    case Int64Regex() => Int64Type
    case UInt8Regex() => UInt8Type
    case UInt16Regex() => UInt16Type
    case UInt32Regex() => UInt32Type
    case UInt64Regex() => UInt64Type
    case FloatRegex() => Float32Type
    case DoubleRegex() => Float64Type
    case DateRegex() => DateType
    case NullRegex() => NullType
    case typeStr =>
      throw new IllegalArgumentException(s"Unknown basic type `$typeStr` is not supported.")
  }

  def fromJson(node: JsonNode): DataType = {
    val nodeType = node.getNodeType
    nodeType match {
      // For Basic Types
      case JsonNodeType.STRING =>
        fromBasicType(node.textValue())

      // For Time Type
      case JsonNodeType.OBJECT
          if node.hasNonNull("Time") || node.hasNonNull("Time32") || node.hasNonNull("Time64") =>
        Seq(node.get("Time"), node.get("Time32"), node.get("Time64"))
          .map(Option(_))
          .collectFirst { case Some(v) => v } match {
          case Some(timeUnit) => TimeType(timeUnit.textValue())
          case None => throw new IllegalArgumentException("Invalid time cannot be parsed.")
        }

      // For Duration Type
      case JsonNodeType.OBJECT if node.hasNonNull("Duration") =>
        Duration(node.get("Duration").textValue())

      // For Decimal Type
      case JsonNodeType.OBJECT if node.has("Decimal") =>
        val decNode = node.get("Decimal")
        if (decNode.isNull || decNode.isMissingNode) {
          DecimalType(None, 0)
        } else {
          val elements = decNode.elements().asScala.toSeq
          val precision =
            if (elements.nonEmpty && !elements.head.isNull) Some(elements.head.asInt()) else None
          val scale = if (elements.length > 1) elements(1).asInt() else 0
          DecimalType(precision, scale)
        }

      // For DateTime Type
      case JsonNodeType.OBJECT if node.hasNonNull("Timestamp") =>
        node.get("Timestamp").elements().asScala.map(_.asText(null)).toSeq match {
          case Seq(tu, tz) => DateTimeType(tu, tz)
          case _ => DateTimeType(null, null)
        }

      // For (Nested) List Type
      case JsonNodeType.OBJECT if node.hasNonNull("List") || node.hasNonNull("LargeList") =>
        val listNode = Option(node.get("List")).getOrElse(node.get("LargeList"))
        ListType(fromJson(listNode.get("dtype")))

      // For (Nested) Struct Type
      case JsonNodeType.OBJECT if node.has("Struct") =>
        val sf = node
          .get("Struct")
          .iterator()
          .asScala
          .map { fieldNode =>
            Field(fieldNode.get("name").textValue(), fromJson(fieldNode.get("dtype")))
          }
          .toArray
        StructType(sf)

      case _ =>
        throw new IllegalArgumentException(s"Invalid field type `$nodeType` cannot be parsed.")
    }
  }

  def typeToDataType[T: ClassTag](): DataType = {
    val clazz = implicitly[ClassTag[T]].runtimeClass
    clazz match {
      case c if c == classOf[java.lang.Byte] || c == classOf[Byte] => Int8Type
      case c if c == classOf[java.lang.Short] || c == classOf[Short] => Int16Type
      case c if c == classOf[java.lang.Integer] || c == classOf[Int] => Int32Type
      case c if c == classOf[java.lang.Long] || c == classOf[Long] => Int64Type
      case c if c == classOf[java.lang.Boolean] || c == classOf[Boolean] => BooleanType
      case c if c == classOf[java.lang.Float] || c == classOf[Float] => Float32Type
      case c if c == classOf[java.lang.Double] || c == classOf[Double] => Float64Type
      case c if c == classOf[java.time.LocalDate] => DateType
      case c if c == classOf[java.time.LocalTime] => TimeType
      case c if c == classOf[java.time.ZonedDateTime] => DateTimeType(null, null)
      case c if c == classOf[java.lang.String] || c == classOf[String] => StringType
      case c if c == classOf[java.util.List[_]] => ListType(null)
      case c =>
        throw new IllegalArgumentException(
          s"Data type could not be found for class `${c.getSimpleName}`"
        )
    }
  }

  /** Helper to represent [[DataType]] as a tree string. */
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
