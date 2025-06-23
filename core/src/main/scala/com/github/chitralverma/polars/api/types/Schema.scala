package com.github.chitralverma.polars.api.types

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.github.chitralverma.polars.jsonMapper

case class Field(name: String, dataType: DataType) {

  /** Borrowed from Apache Spark source to represent [[Field]] as a tree string. */
  private[polars] def buildFormattedString(prefix: String, buffer: StringBuffer): Unit = {
    buffer.append(s"$prefix-- $name: ${dataType.simpleName} \n")
    DataType.buildFormattedString(dataType, s"$prefix    |", buffer)
  }

}

class Schema {

  private var _fields: Array[Field] = _

  def getFields: Array[Field] = _fields

  def getFieldNames: Array[String] = _fields.map(f => f.name)

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

  private def toField(field: (String, JsonNode, JsonNodeType)): Field = field match {
    // For Basic Types
    case (name, node, _ @JsonNodeType.STRING) =>
      Field(name, DataType.fromBasicType(node.textValue()))

    // For Time Type
    case (name, node, _ @JsonNodeType.OBJECT)
        if node.hasNonNull("Time") || node.hasNonNull("Time32") || node.hasNonNull("Time64") =>
      Seq(node.get("Time"), node.get("Time32"), node.get("Time64"))
        .map(Option(_))
        .collectFirst { case Some(v) => v } match {
        case Some(timeUnit) => Field(name, TimeType(timeUnit.textValue()))

        case None =>
          throw new IllegalArgumentException("Invalid time cannot be parsed.")
      }

    // For Duration Type
    case (name, node, _ @JsonNodeType.OBJECT) if node.hasNonNull("Duration") =>
      val timeUnit = node.get("Duration")
      Field(name, Duration(timeUnit.textValue()))

    // For DateTime Type
    case (name, node, _ @JsonNodeType.OBJECT) if node.hasNonNull("Timestamp") =>
      node.get("Timestamp").elements().asScala.map(_.asText(null)).toSeq match {
        case Seq(tu, tz) =>
          Field(name, DateTimeType(tu, tz))
        case _ =>
          Field(name, DateTimeType(null, null))
      }

    // For (Nested) List Type
    case (name, node, _ @JsonNodeType.OBJECT)
        if node.hasNonNull("List") || node.hasNonNull("LargeList") =>
      Seq(node.get("List"), node.get("LargeList"))
        .map(Option(_))
        .collectFirst { case Some(v) => v } match {
        case Some(listNode) =>
          val listNodeType = listNode.get("dtype")
          Field(name, ListType(toField((name, listNodeType, listNodeType.getNodeType)).dataType))

        case None =>
          throw new IllegalArgumentException("Invalid list cannot be parsed as a JSON.")
      }

    // For (Nested) Struct Type
    case (name, node, _ @JsonNodeType.OBJECT) if node.has("Struct") =>
      val structNode = node.get("Struct")
      val structFields = structNode.iterator().asScala
      val sf = structFields.map {
        case node: JsonNode if node.hasNonNull("name") && node.hasNonNull("dtype") =>
          val structFieldName: String = node.get("name").textValue()
          val structFieldType: JsonNode = node.get("dtype")

          Field(
            structFieldName,
            toField(name, structFieldType, structFieldType.getNodeType).dataType
          )

        case _ =>
          throw new IllegalArgumentException("Invalid struct cannot be parsed as a JSON.")
      }.toArray

      Field(name, StructType(sf))

    case _ =>
      throw new IllegalArgumentException("Invalid field cannot be parsed as a JSON.")
  }

  private def setFields(fields: Array[Field]): Schema = {
    fields match {
      case f if f == null || f.isEmpty =>
        throw new IllegalArgumentException("Provided fields cannot be null or empty.")

      case _ =>
        _fields = fields
    }

    this
  }

  private def deserialize(json: String): Schema = {
    Try(jsonMapper.reader.readTree(json)).toOption match {
      case None =>
        throw new IllegalArgumentException("Provided schema string cannot be parsed as a JSON.")

      case Some(node: JsonNode) if node.hasNonNull("fields") =>
        val fields = node.get("fields").elements().asScala.toList
        _fields = fields
          .map(f =>
            toField(f.get("name").textValue(), f.get("dtype"), f.get("dtype").getNodeType)
          )
          .toArray

      case _ =>
        throw new IllegalArgumentException("Provided schema string is an invalid JSON.")
    }

    this
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
  def fromString(jsonString: String): Schema = new Schema().deserialize(jsonString)

  def fromFields(fields: Array[Field]): Schema = new Schema().setFields(fields)
}
