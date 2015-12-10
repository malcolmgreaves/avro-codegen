package com.nitro.scalaAvro.codegen

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

import RichAvro._

object AvroRecord {
  def isRecord(schema: Schema) =
    schema.getType == Schema.Type.RECORD

  def fromSchema(schema: Schema): Option[AvroRecord] =
    if (isRecord(schema))
      Some(AvroRecord(schema))
    else None
}

case class AvroRecord(record: Schema) {
  if (!AvroRecord.isRecord(record)) throw new Exception("schema not of record type")

  def fields = record.getFields

  def nameSpace = Option(record.getNamespace).getOrElse(AvroGenerator.defaultNameSpace)

  val nameSymbol = record.getName.asSymbol
  def mutableNameSymbol = (record.getName+"Mutable").asSymbol

  def baseClasses: Seq[String] =
    Seq(
      "com.nitro.scalaAvro.runtime.GeneratedMessage",
      s"com.nitro.scalaAvro.runtime.Message[$upperScalaName]"
    )

  val scalaName: String = snakeCaseToCamelCase(nameSymbol)

  val upperScalaName: String = snakeCaseToCamelCase(nameSymbol, true)

  def avroType = record.getType

  def scalaTypeName: String = upperScalaName

  def fullScalaTypeName: String =
    record.fullScalaTypeName

  def getName: String = record.getName
}

//fallback for schema parsing only
object AvroAnyUnion {
  def isAnyUnion(schema: Schema) =
    schema.getType == UNION

  def fromSchema(schema: Schema): Option[AvroAnyUnion] =
    if (isAnyUnion(schema))
      Some(AvroAnyUnion(schema))
    else None
}

case class AvroAnyUnion(schema: Schema) {
  if (!AvroAnyUnion.isAnyUnion(schema)) throw new Exception("schema not of any union type")

  val types = schema.getTypes
}

object AvroOptionalUnion {
  def isOptionalUnion(schema: Schema) =
    schema.getType == UNION &&
      schema.getTypes.map(_.getType).contains(NULL) &&
      schema.getTypes.size > 2

  def fromSchema(schema: Schema): Option[AvroOptionalUnion] =
    if (isOptionalUnion(schema))
      Some(AvroOptionalUnion(schema))
    else None
}

case class AvroOptionalUnion(schema: Schema) {
  if (!AvroOptionalUnion.isOptionalUnion(schema)) throw new Exception("schema not of optional union type")

  val types = schema.getTypes.filter(_.getType != NULL)

  def isOversize = types.length >= AvroUnion.maxUnionSize
  def unionScalaTypes = types.map(_.fullScalaTypeName)

  val alpha = (('a' to 'z') ++ ('A' to 'Z')).toSet
  def unionTypeName = if (isOversize) "Any"
  else unionScalaTypes.foldRight("shapeless.CNil")((t, acc) => s"shapeless.:+:[$t, $acc]")
  def optionalUnionTypeName = s"Option[$unionTypeName]"
  def unionTypeExtractorName = unionScalaTypes.mkString("").filter(alpha)+"Extractor"
  def unionTypeBuilderName = "build"+unionScalaTypes.mkString("").filter(alpha)
}

object AvroUnion {
  val maxUnionSize = 5

  def isUnion(schema: Schema) =
    schema.getType == UNION &&
      !schema.getTypes.map(_.getType).contains(NULL) &&
      schema.getTypes.size >= 2

  def fromSchema(schema: Schema): Option[AvroUnion] =
    if (isUnion(schema))
      Some(AvroUnion(schema))
    else None
}

case class AvroUnion(schema: Schema) {
  if (!AvroUnion.isUnion(schema)) throw new Exception("schema not of union type")

  val types = schema.getTypes
  def isOversize = types.length >= AvroUnion.maxUnionSize
  def unionScalaTypes = types.map(_.fullScalaTypeName)

  val alpha = (('a' to 'z') ++ ('A' to 'Z')).toSet

  def unionTypeName = if (isOversize) "Any"
  else unionScalaTypes.foldRight("shapeless.CNil")((t, acc) => s"shapeless.:+:[$t, $acc]")
  def unionTypeExtractorName = unionScalaTypes.mkString("").filter(alpha)+"Extractor"
  def unionTypeBuilderName = "build"+unionScalaTypes.mkString("").filter(alpha)
}

object AvroOptional {
  def isOptional(schema: Schema) =
    schema.getType == UNION &&
      schema.getTypes.map(_.getType).contains(NULL) &&
      schema.getTypes.size == 2

  def fromSchema(schema: Schema): Option[AvroOptional] =
    if (isOptional(schema))
      Some(AvroOptional(schema))
    else None
}

case class AvroOptional(schema: Schema) {
  if (!AvroOptional.isOptional(schema)) throw new Exception("schema not of 2-element optional union type")

  val nonNullSchema = schema.getTypes.filter(_.getType != NULL).head

  def nestedTypeName = s"${nonNullSchema.fullScalaTypeName}"

  def scalaTypeName = s"Option[$nestedTypeName]"
}

object AvroEnum {
  def isEnum(schema: Schema) = schema.getType == Schema.Type.ENUM
  def fromSchema(schema: Schema): Option[AvroEnum] =
    if (isEnum(schema)) Some(AvroEnum(schema))
    else None
}

case class AvroEnum(schema: Schema) {
  if (!AvroEnum.isEnum(schema)) throw new Exception("schema not of enum type")

  def nameSpace = Option(schema.getNamespace).getOrElse(AvroGenerator.defaultNameSpace)

  def getValues: Seq[AvroEnumValue] =
    schema.getEnumSymbols.map { s =>
      //could probably assume they're ordered, actually.
      AvroEnumValue(s, schema.getEnumOrdinal(s))
    }

  def getName: String = schema.getName
}

case class AvroEnumValue(name: String, number: Int)

object AvroString {
  def isString(schema: Schema) = schema.getType == Schema.Type.STRING
  def fromSchema(schema: Schema): Option[AvroString] =
    if (isString(schema)) Some(AvroString(schema))
    else None
}

case class AvroString(schema: Schema) {
  if (!AvroString.isString(schema)) throw new Exception("schema not of string type")
}

object AvroMap {
  def isMap(schema: Schema) = schema.getType == Schema.Type.MAP
  def fromSchema(schema: Schema): Option[AvroMap] =
    if (isMap(schema)) Some(AvroMap(schema))
    else None
}

case class AvroMap(schema: Schema) {
  if (!AvroMap.isMap(schema)) throw new Exception("schema not of map type")

  def valueType = schema.getValueType

  def getName: String = schema.getName

  def scalaTypeName = s"Map[String, ${valueType.fullScalaTypeName}]"
}

object AvroArray {
  def isArray(schema: Schema) = schema.getType == Schema.Type.ARRAY
  def fromSchema(schema: Schema): Option[AvroArray] =
    if (isArray(schema)) Some(AvroArray(schema))
    else None
}

case class AvroArray(schema: Schema) {
  if (!AvroArray.isArray(schema)) throw new Exception("schema not of array type")

  def elementType = schema.getElementType

  def getName: String = schema.getName

  def scalaTypeName = s"Vector[${elementType.fullScalaTypeName}]"
}
