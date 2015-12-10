package com.nitro.scalaAvro.codegen

import org.apache.avro.Schema.Field
import org.apache.avro.Schema.Type._
import org.apache.avro.Schema

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.Try

/**
 * implicit wrappers for avro builtin classes
 */
object RichAvro {

  implicit class RichSchema(schema: Schema) {

    def avroType = schema.getType

    def recordName = asRecord.map(_.upperScalaName)
    def enumName: Option[String] = if (avroType == ENUM) Some(schema.getName) else None

    def asArray = AvroArray.fromSchema(schema)
    def asUnion = AvroUnion.fromSchema(schema)
    def asAnyUnion = AvroAnyUnion.fromSchema(schema)
    def asOptionalUnion = AvroOptionalUnion.fromSchema(schema)
    def asMap = AvroMap.fromSchema(schema)
    def asRecord = AvroRecord.fromSchema(schema)
    def asString = AvroString.fromSchema(schema)
    def asEnum = AvroEnum.fromSchema(schema)
    def asOptional = AvroOptional.fromSchema(schema)

    def unionTypeName = asUnion.map(_.unionTypeName)

    def optionalTypeName = asOptional.map(_.scalaTypeName)

    def optionalUnionTypeName = asOptionalUnion.map(_.optionalUnionTypeName)

    def mapTypeName = asMap.map(_.scalaTypeName)

    def arrayTypeName = asArray.map(_.scalaTypeName)

    def scalaTypeName: String =
      Types.scalaTypeName(avroType) //primitive type name
        .orElse(recordName)
        .orElse(enumName)
        .orElse(optionalTypeName)
        .orElse(unionTypeName)
        .orElse(mapTypeName)
        .orElse(arrayTypeName)
        .orElse(optionalUnionTypeName)
        .getOrElse("???")

    /**
     * Evaluates to this schema's full Scala type: package name space and
     * the object, class, or trait name.
     */
    def fullScalaTypeName: String =
      nameSpace match {
        case Some(ns) =>
          s"$ns.$scalaTypeName"

        case None =>
          s"$scalaTypeName"
      }

    /**
     * Obtains this schema's name space, if possible. If this schema doesn't
     * have a name space, then None is returned: indicating that the type is
     * built-in.
     */
    def nameSpace: Option[String] =
      Try {
        // When the schema is for a "built-in type" (e.g. Boolean, String, ...)
        // then schema.getNamespace throws an exception (yes terrible, but
        // it is how the Avro project decided to do this :( ).
        // So when we encounter an exception, the idea is that there is "no
        // name space" i.e. a None.
        //
        // When schema.getNamespace is called on a Schema for a user-defined
        // type that doesn't have a namespace field in the schema, then the
        // method returns null. In this case, we want to evaluate to the
        // default name space.
        val namespace = schema.getNamespace
        if (namespace == null || namespace.isEmpty)
          AvroGenerator.defaultNameSpace
        else
          namespace
      }
        .toOption

    def isEnum = avroType == ENUM
    def isString = avroType == STRING
    def isRecord = avroType == RECORD
  }

  implicit class RichField(field: Field) {

    def scalaName: String = snakeCaseToCamelCase(field.name())

    def upperScalaName: String =
      snakeCaseToCamelCase(field.name(), upperInitial = true)

    def avroType = field.schema().avroType

    /**
     * Evaluates to this field's full Scala type: package name space and
     * the object, class, or trait name.
     */
    def scalaTypeName: String =
      field.schema().fullScalaTypeName

    def isEnum = avroType == ENUM
    def isString = avroType == STRING
    def isRecord = avroType == RECORD
    def isMap = avroType == MAP
    def isUnion = avroType == UNION
  }
}
