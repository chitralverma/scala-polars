package org.polars.scala.polars.api.types

import java.time.ZoneId
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.util.Try

import org.json4s._
import org.json4s.native.JsonMethods.parseOpt
import org.polars.scala.polars.formats

case class Field(name: String, dataType: DataType) {

  /** Borrowed from Apache Spark source to represent [[Field]] as a tree string. */
  private[polars] def buildFormattedString(prefix: String, buffer: StringBuffer): Unit = {
    buffer.append(s"$prefix-- $name: ${dataType.simpleName} \n")
    DataType.buildFormattedString(dataType, s"$prefix    |", buffer)
  }

}

class Schema private (private[polars] val json: String) {

  private var _fields: Array[Field] = _
  private var _fieldNames: Array[String] = _

  def getFields: Array[Field] = _fields

  def getFieldNames: Array[String] = _fieldNames

  def getField(i: Int): Option[Field] = Try(getFields(i)).toOption

  def getField(name: String, ignoreCase: Boolean = false): Option[Field] =
    getFields.find { field =>
      val fieldName = field.name
      if (ignoreCase) fieldName.equalsIgnoreCase(name)
      else fieldName.equals(name)
    }

  def getFieldIndex(name: String, ignoreCase: Boolean = false): Option[Int] =
    getField(name, ignoreCase).map(f => getFields.indexOf(f))

  override def toString: String = treeString

  deserialize()

  private def toField(field: JField): Field = field match {
    // For Basic Types
    case (name, _ @JString(t)) =>
      Field(name, DataType.fromBasicType(t))

    // For DateTime Type
    case (name, _ @JObject(Seq(JField("Datetime", v)))) =>
      val (tu, tz) = v.extract[Seq[String]] match {
        case Seq(null, null) =>
          (TimeUnit.MICROSECONDS, ZoneId.of("UTC"))
        case Seq(null, tz) if tz.nonEmpty =>
          (TimeUnit.MICROSECONDS, ZoneId.of(tz))
        case Seq(tu, null) if tu.nonEmpty =>
          (TimeUnit.valueOf(tu.toUpperCase(Locale.ROOT)), ZoneId.of("UTC"))
        case Seq(tu, tz) if tu.nonEmpty && tz.nonEmpty =>
          (TimeUnit.valueOf(tu.toUpperCase(Locale.ROOT)), ZoneId.of(tz))
        case _ =>
          (TimeUnit.MICROSECONDS, ZoneId.of("UTC"))
      }

      Field(name, DateTimeType(tu, tz))

    // For (Nested) List Type
    case (name, _ @JObject(Seq(JField("List", v)))) =>
      Field(name, ListType(toField(JField(name, v)).dataType))

    // For (Nested) Struct Type
    case (name, _ @JObject(Seq(JField("Struct", JArray(structFields))))) =>
      val sf = structFields.map {
        case JObject(Seq(JField("name", JString(structFieldName)), JField("dtype", v))) =>
          Field(structFieldName, toField(JField(name, v)).dataType)

        case _ =>
          throw new IllegalArgumentException("Invalid struct cannot be parsed as a JSON.")
      }

      Field(name, StructType(sf))

    case _ =>
      throw new IllegalArgumentException("Invalid field cannot be parsed as a JSON.")
  }

  private def deserialize(): Unit = parseOpt(json) match {
    case None =>
      throw new IllegalArgumentException("Provided schema string cannot be parsed as a JSON.")

    case Some(JObject(Seq(("inner", _ @JObject(fields))))) =>
      _fields = fields.map(toField).toArray
      _fieldNames = _fields.map(_.name)

    case _ =>
      throw new IllegalArgumentException("Provided schema string is an invalid JSON.")
  }

  /** Borrowed from Apache Spark source to represent Schema as a tree string. */
  private[polars] def treeString: String = {
    val stringBuffer = new StringBuffer()
    stringBuffer.append("root\n")
    val prefix = " |"
    getFields.foreach(field => field.buildFormattedString(prefix, stringBuffer))

    stringBuffer.toString
  }

}

object Schema {
  def from(jsonString: String): Schema = new Schema(jsonString)
}
