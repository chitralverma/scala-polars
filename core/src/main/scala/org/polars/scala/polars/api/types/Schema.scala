package org.polars.scala.polars.api.types

import java.time.ZoneId
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._
import scala.util.Try

import org.polars.scala.polars.jsonMapper

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeType

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

  def toField(field: (String, JsonNode, JsonNodeType)): Field = field match {
    // For Basic Types
    case (name, node, _ @JsonNodeType.STRING) =>
      Field(name, DataType.fromBasicType(node.textValue()))

    // For DateTime Type
    case (name, node, _ @JsonNodeType.OBJECT) if node.has("Datetime") =>
      // todo: validate the timeunit and timezone and re-enable this later

//      val dtNode = node.get("Datetime")
//
//      val (tu, tz) = dtNode.iterator().asScala.map(_.textValue()).toSeq match {
//        case Seq(null, null) =>
//          (TimeUnit.MICROSECONDS, ZoneId.of("UTC"))
//        case Seq(null, tz) if tz.nonEmpty =>
//          (TimeUnit.MICROSECONDS, ZoneId.of(tz))
//        case Seq(tu, null) if tu.nonEmpty =>
//          (TimeUnit.valueOf(tu.toUpperCase(Locale.ROOT)), ZoneId.of("UTC"))
//        case Seq(tu, tz) if tu.nonEmpty && tz.nonEmpty =>
//          (TimeUnit.valueOf(tu.toUpperCase(Locale.ROOT)), ZoneId.of(tz))
//        case _ =>
//          (TimeUnit.MICROSECONDS, ZoneId.of("UTC"))
//      }

      Field(name, DateTimeType)

    // For (Nested) List Type
    case (name, node, _ @JsonNodeType.OBJECT) if node.has("List") =>
      val listNode = node.get("List")
      Field(name, ListType(toField((name, listNode, listNode.getNodeType)).dataType))

    // For (Nested) Struct Type
    case (name, node, _ @JsonNodeType.OBJECT) if node.has("Struct") =>
      val structNode = node.get("Struct")
      val structFields = structNode.iterator().asScala
      val sf = structFields.map {
        case node: JsonNode if node.fieldNames().asScala.toSet == Set("name", "dtype") =>
          val structFieldName: String = node.get("name").textValue()
          val structFieldType: JsonNode = node.get("dtype")

          Field(
            structFieldName,
            toField(name, structFieldType, structFieldType.getNodeType).dataType
          )

        case _ =>
          throw new IllegalArgumentException("Invalid struct cannot be parsed as a JSON.")
      }.toSeq

      Field(name, StructType(sf))

    case _ =>
      throw new IllegalArgumentException("Invalid field cannot be parsed as a JSON.")
  }

  private def deserialize(): Unit = Try(jsonMapper.reader.readTree(json)).toOption match {
    case None =>
      throw new IllegalArgumentException("Provided schema string cannot be parsed as a JSON.")

    case Some(node: JsonNode) if node.has("inner") =>
      val fields = node.get("inner").fields().asScala
      _fields = fields.map(f => toField(f.getKey, f.getValue, f.getValue.getNodeType)).toArray
      _fieldNames = node.fieldNames().asScala.toArray

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
