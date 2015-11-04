package com.nitro.scalaAvro.codegen

import scala.collection.JavaConversions._
import org.apache.avro.Schema.Type
import org.apache.avro.Schema
import org.apache.avro.Schema.Field

import RichAvro._

case class AvroGeneratorParams(javaConversions: Boolean = false, flatPackage: Boolean = false)

class AvroGenerator(val params: AvroGeneratorParams = AvroGeneratorParams()) extends AvroExpressions {

  def printRecord(record: AvroRecord)(printer: FunctionalPrinter): FunctionalPrinter =
    printer
      .add(s"final case class ${record.upperScalaName}(")
      .indent
      .indent
      .call(printConstructorFieldList(record))
      .outdent
      .add(s") extends ${record.baseClasses.mkString(" with ")} {")
      .indent
      .print(record.fields) {
        case (field, printer) =>
          val withMethod = "with" + field.upperScalaName
          printer
            .add(s"def $withMethod(__v: ${field.scalaTypeName}): ${record.upperScalaName} = copy(${field.scalaName.asSymbol} = __v)")
      }
      .print(nestedSchemas(record)) { (schema, printer) =>
        printer
          .call(generateUnionExtractor(schema))
      }
      .call(generateToMutable(record))
      .add(s"def companion = ${record.scalaTypeName}")
      .outdent
      .add("}")
      .outdent
      .call(generateMessageCompanion(record))

  def printMutableFields(record: AvroRecord)(printer: FunctionalPrinter): FunctionalPrinter =
    printer
      .print(record.fields) {
        case (field, printer) =>
          val typeName = field.scalaTypeName
          val varDef = s"var ${field.scalaName.asSymbol}: $typeName = _" //$ctorDefaultValue", but let's ignore defaults for now
          printer.add(varDef)
      }

  def generateArbitrary(record: AvroRecord)(printer: FunctionalPrinter): FunctionalPrinter = {
    val arbType = s"org.scalacheck.Gen[${record.record.scalaTypeName}]"
    printer.
      add(s"val _arbitrary: $arbType = for {")
      .indent
      .print(record.fields) { (field, printer) =>
        printer.add(s"${field.name.asSymbol} <- ").call(genFor(field.schema()))
      }
      .outdent
      .add(s"} yield ${record.upperScalaName}(")
      .indent
      .printWithDelimiter(record.fields, ",") { (field, printer) =>
        printer.add(s"${field.scalaName.asSymbol} = ${field.name.asSymbol} ")
      }
      .outdent
      .add(")")
  }

  def generateMessageCompanion(record: AvroRecord)(printer: FunctionalPrinter): FunctionalPrinter = {
    val className = record.upperScalaName
    val companionType = s"com.nitro.scalaAvro.runtime.GeneratedMessageCompanion[$className]"
    printer.addM(
      s"""object $className extends $companionType {
         |  implicit def messageCompanion: $companionType = this
         |  def schema: org.apache.avro.Schema =
         |    new org.apache.avro.Schema.Parser().parse(\"\"\"${record.record.toString}\"\"\")""" //supply schema as json
    )
      .indent
      .print(nestedSchemas(record)) { (schema, printer) =>
        printer.call(generateUnionBuilder(schema))
      }
      .print(record.fields) { (field, printer) =>
        printer
          .call(generateUnionTypeAlias(field))
      }
      .call(generateArbitrary(record))
      .call(generateFromMutable(record))
      .outdent
      .add("}")
      .add("")
  }

  //extract union builder/extractor for nested union types.
  //ex: Vector[Map[String, UnionXYZ]], UnionXYZ needs generated extractor/builder
  //recursive nested schema extractor (skipping records, which may have their own union children, w/ generation elsewhere)
  def nestedSchemas(record: AvroRecord): Seq[Schema] = {
    def nestedSchema(schema: Schema): Seq[Schema] =
      schema.asMap.map { map =>
        schema +: nestedSchema(map.valueType)
      }.orElse(schema.asArray.map { arr =>
        schema +: nestedSchema(arr.elementType)
      }).orElse(schema.asOptional.map { opt =>
        schema +: nestedSchema(opt.nonNullSchema)
      }).orElse(schema.asOptionalUnion.map { optU =>
        schema +: optU.types.flatMap(nestedSchema)
      }).orElse(schema.asUnion.map { union =>
        schema +: union.types.flatMap(nestedSchema)
      }).getOrElse(Seq.empty)
    record.fields.map(_.schema()).flatMap(nestedSchema)
  }

  def generateUnionBuilder(schema: Schema)(printer: FunctionalPrinter): FunctionalPrinter = {
    def builderFor(types: Seq[Schema], unionTypeName: String, unionTypeBuilderName: String, isOversize: Boolean)(printer: FunctionalPrinter) =
      printer
        .add(s"def $unionTypeBuilderName(__a: Any): Option[$unionTypeName] = __a match {")
        .indent
        .print(types) { (schema, printer) =>

          def resultWrapper(expr: String) = if (isOversize) s"Some($expr)"
          else s"Some(shapeless.Coproduct[$unionTypeName]($expr))"

          val asScalaCase = schema.getType match {
            case Type.RECORD =>
              s"""case (__record: ${Types.GenericRecordBase}) if __record.getSchema.getName == "${schema.getName}" => ${resultWrapper(fromMutable(schema)("__record"))}"""
            case Type.UNION => throw new Exception("Avro Unions may not immediately contain other unions")
            case Type.ENUM =>
              s"""case (__enum: ${Types.GenericEnumBase}) if (__enum.getSchema.getName == "${schema.getName}") => ${resultWrapper(fromMutable(schema)("__enum"))}"""
            case Type.STRING =>
              s"""case (__string: ${Types.Utf8}) => ${resultWrapper(fromMutable(schema)("__string"))}"""
            case Type.ARRAY =>
              s"""case (__arr: ${Types.GenericArrayBase}[_]) => ${resultWrapper(fromMutable(schema)("__arr"))}"""
            case Type.MAP =>
              s"""case (__map: ${Types.GenericMap}[_,_]) => ${resultWrapper(fromMutable(schema)("__map"))}"""
            case f =>
              s"case (__x: ${schema.scalaTypeName}) => ${resultWrapper(fromMutable(schema)("__x"))}"
          }
          printer.add(asScalaCase)
        }.outdent.add("}")

    schema.asUnion.map { union =>
      printer.call(builderFor(union.types, unionTypeName = union.unionTypeName, unionTypeBuilderName = union.unionTypeBuilderName, union.isOversize))
    }.orElse(schema.asOptionalUnion.map { optU =>
      printer.call(builderFor(optU.types, unionTypeName = optU.unionTypeName, unionTypeBuilderName = optU.unionTypeBuilderName, optU.isOversize))
    }).getOrElse(printer)
  }

  def generateUnionTypeAlias(field: Field)(printer: FunctionalPrinter): FunctionalPrinter =
    field.schema.asUnion.map { union =>
      if (union.isOversize) printer
      else printer.add(s"type ${field.upperScalaName}Type = ${union.unionTypeName}")
    }.orElse(field.schema.asOptionalUnion.map { optU =>
      if (optU.isOversize) printer
      else printer.add(s"type ${field.upperScalaName}Type = ${optU.unionTypeName}")
    }).getOrElse(printer)

  def generateUnionExtractor(schema: Schema)(printer: FunctionalPrinter): FunctionalPrinter = {
    def polyExtractorFor(types: Seq[Schema], unionTypeExtractorName: String)(printer: FunctionalPrinter) =
      printer
        .add(s"private object $unionTypeExtractorName extends shapeless.Poly1 {")
        .indent
        .print(types.zipWithIndex) {
          case ((schema, idx), printer) =>
            printer
              .add(s"implicit def case$idx = at[${schema.scalaTypeName}]{ x => ")
              .indent
              .add(toMutable(schema)("x"))
              .outdent
              .add("}")
        }.outdent.add("}")

    def extractorFor(types: Seq[Schema], unionTypeExtractorName: String)(printer: FunctionalPrinter) =
      printer
        .add(s"def $unionTypeExtractorName(_x: Any) = _x match {")
        .indent
        .print(types) {
          case (schema, printer) =>
            printer
              .add(s"case (__x: ${schema.scalaTypeName}) => ")
              .indent
              .add(toMutable(schema)("__x"))
              .outdent
        }.outdent.add("}")

    schema.asUnion.map { union =>
      if (union.isOversize) printer.call(extractorFor(union.types, unionTypeExtractorName = union.unionTypeExtractorName))
      else printer.call(polyExtractorFor(union.types, unionTypeExtractorName = union.unionTypeExtractorName))
    }.orElse(schema.asOptionalUnion.map { optU =>
      if (optU.isOversize) printer.call(extractorFor(optU.types, unionTypeExtractorName = optU.unionTypeExtractorName))
      else printer.call(polyExtractorFor(optU.types, unionTypeExtractorName = optU.unionTypeExtractorName))
    }).getOrElse(printer)
  }

  def generateToMutable(record: AvroRecord)(printer: FunctionalPrinter): FunctionalPrinter = {
    printer
      .add(s"def toMutable: ${Types.GenericRecordInterface} = {")
      .indent
      .add(s"val __out__ = new ${Types.GenericRecordBase}(${record.scalaTypeName}.schema)")
      .print(record.fields) {
        case (field, printer) =>
          val asGeneric = toMutable(field.schema)(field.scalaName.asSymbol)
          printer.add(s"""__out__.put("${field.name()}", $asGeneric)""")
      }
      .add("__out__")
      .outdent.add("}")
  }

  def generateFromMutable(enum: AvroEnum)(printer: FunctionalPrinter): FunctionalPrinter = {
    printer
      .add(s"def fromMutable(generic: ${Types.GenericEnumInterface}): ${enum.getName.asSymbol} = generic.toString match {")
      .print(enum.getValues) {
        case (v, p) => p.add(s"""  case "${v.name}" => ${v.name.asSymbol}""")
      }.add("}")
  }

  def generateFromMutable(record: AvroRecord)(printer: FunctionalPrinter): FunctionalPrinter = {
    val fields = record.fields.map { f =>
      val extracted = s"""generic.get("${f.name()}")"""
      val converted = fromMutable(f.schema())(extracted)
      s"${f.scalaName.asSymbol} = $converted"
    }

    printer
      .add(s"def fromMutable(generic: ${Types.GenericRecordInterface}): ${record.upperScalaName} = ")
      .indent
      .add(s"${record.upperScalaName}(")
      .indent
      .addWithDelimiter(", ")(fields)
      .outdent
      .add(")")
      .outdent
  }

  def printConstructorFieldList(record: AvroRecord)(printer: FunctionalPrinter): FunctionalPrinter = {
    val regularFields = record.fields.collect {
        case field =>
          val fieldName = field.scalaName.asSymbol
          val typeName = field.scalaTypeName
          s"$fieldName: $typeName"
    }
    printer.addWithDelimiter(",")(regularFields)
  }

  def printEnum(e: AvroEnum)(printer: FunctionalPrinter): FunctionalPrinter = {
    val name = e.getName.asSymbol
    printer
      .add(s"sealed trait $name extends com.nitro.scalaAvro.runtime.GeneratedEnum {")
      .indent
      .print(e.getValues) {
        case (v, p) => p.add(
          s"def is${v.name}: Boolean = false"
        )
      }
      .outdent
      .add("}")
      .add("")
      .add(s"object $name extends com.nitro.scalaAvro.runtime.GeneratedEnumCompanion[$name] {")
      .indent
      .addM(
        s"""
           |  def schema: org.apache.avro.Schema =
           |    new org.apache.avro.Schema.Parser().parse(\"\"\"${e.schema.toString}\"\"\")"""
      )
      .print(e.getValues) {
        case (v, p) => p.addM(
          s"""case object ${v.name.asSymbol} extends $name {
           |  val id = ${v.number}
           |  val name = "${v.name}"
           |  override def is${v.name}: Boolean = true
           |}
           |"""
        )
      }
      .add(s"lazy val values = Seq(${e.getValues.map(_.name.asSymbol).mkString(", ")})")
      .add(s"def fromValue(id: Int): $name = id match {")
      .print(e.getValues) {
        case (v, p) => p.add(s"  case ${v.number} => ${v.name.asSymbol}")
      }.add("}")
      .call(generateFromMutable(e))
      .add(s"val _arbitrary: org.scalacheck.Gen[$name] = org.scalacheck.Gen.oneOf(Seq(")
      .add(e.getValues.map(_.name.asSymbol).mkString(", "))
      .add("))")
      .outdent
      .add("}")
  }

}

trait AvroExpressions {

  def genFor(schema: Schema)(printer: FunctionalPrinter): FunctionalPrinter = {

    val Arb = "org.scalacheck.Arbitrary"
    val Gen = "org.scalacheck.Gen"
    val GenUtils = "com.nitro.scalaAvro.runtime.AvroGenUtils"
    val genString = s"$GenUtils.genAvroString"

    def etc = schema.avroType match {
      case Type.BOOLEAN => printer.add(s"$Arb.arbBool.arbitrary")
      case Type.INT => printer.add(s"$Arb.arbInt.arbitrary")
      case Type.LONG => printer.add(s"$Arb.arbLong.arbitrary")
      case Type.FLOAT => printer.add(s"$Arb.arbFloat.arbitrary")
      case Type.DOUBLE => printer.add(s"$Arb.arbDouble.arbitrary")
      case Type.BYTES => printer.add(s"$Arb.arbContainer[Array, Byte].arbitrary.map(java.nio.ByteBuffer.wrap)")
      case Type.FIXED => throw new Exception("FIXED type is not supported")
      case Type.NULL => throw new Exception("cannot create org.scalacheck.Gen instance for NULL")
      case x => throw new Exception("unexpected type: " + x)
    }

    schema.asArray.map { array =>
      printer
        .add(s"$GenUtils.genAvroArray(")
        .call(genFor(schema.asArray.get.elementType))
        .add(")")
    }.orElse(schema.asString.map { _ =>
      printer.add(genString)
    }).orElse(schema.asEnum.map { enum =>
      printer.add(s"${enum.getName.asSymbol}._arbitrary")
    }).orElse(schema.asMap.map { map =>
      printer.add(s"$GenUtils.genAvroMap(")
        .call(genFor(map.valueType))
        .add(")")
    }).orElse(schema.asRecord.map { record =>
      printer.add(s"${record.fullScalaTypeName}._arbitrary")
    }).orElse(schema.asOptional.map { opt =>
      printer
        .add(s"$Gen.lzy($Gen.oneOf[${opt.scalaTypeName}](")
        .call(genFor(opt.nonNullSchema))
        .add(".map(Option.apply)")
        .add(", None))")
    }).orElse(schema.asUnion.map { union =>

      def wrapGen(printer: FunctionalPrinter): FunctionalPrinter =
        if (!union.isOversize) printer.add(s".map( elem => shapeless.Coproduct[${union.unionTypeName}](elem))")
        else printer

      printer
        .add(s"$Gen.lzy($Gen.oneOf(")
        .printWithDelimiter(union.types, ",") { (schema, printer) =>
          printer
            .call(genFor(schema))
            .call(wrapGen)
        }.add("))")
    }).orElse(schema.asOptionalUnion.map { optU =>

      def wrapGen(printer: FunctionalPrinter): FunctionalPrinter =
        if (!optU.isOversize) printer.add(s".map( elem => Some(shapeless.Coproduct[${optU.unionTypeName}](elem)))")
        else printer.add(s".map( elem => Some(elem))")

      printer
        .add(s"$Gen.lzy($Gen.oneOf(")
        .add(s"$Gen.const(None),")
        .printWithDelimiter(optU.types, ",") { (schema, printer) =>
          printer
            .call(genFor(schema))
            .call(wrapGen)
        }.add("))")
    }).getOrElse(etc)
  }

  def fromMutable(schema: Schema): String => String = {
    val stringConverter = (s: String) => s"convertString($s)"
    schema.asOptional.map { opt => (s: String) =>
      s"Option($s).map(_e => ${fromMutable(opt.nonNullSchema)("_e")})"
    }.orElse(schema.asUnion.map { union => (s: String) =>
      s"${union.unionTypeBuilderName}($s).getOrElse(throw new Exception())"
    }).orElse(schema.asOptionalUnion.map { optU => (s: String) =>
      //throw if no match found, to avoid confusing legitimate null value with error cases
      s"Option($s).map(_u => ${optU.unionTypeBuilderName}(_u).getOrElse(throw new Exception()))"
    }).orElse(schema.asMap.map { map => (s: String) =>
      s"""scala.collection.immutable.Map() ++ scala.collection.JavaConversions.mapAsScalaMap($s.asInstanceOf[java.util.HashMap[${Types.Utf8}, Any]]).map{ case (_k,_v) => (${stringConverter("_k")}, ${fromMutable(map.valueType)("_v")})}"""
    }).orElse(schema.asEnum.map { enum => (s: String) =>
      s"${enum.getName.asSymbol}.fromMutable($s.asInstanceOf[${Types.GenericEnumInterface}])"
    }).orElse(schema.asRecord.map { record => (s: String) =>
      s"${record.fullScalaTypeName}.fromMutable($s.asInstanceOf[${Types.GenericRecordInterface}])"
    }).orElse(schema.asString.map { string => (s: String) =>
      stringConverter(s)
    }).orElse(schema.asArray.map { array => (s: String) =>
      s"scala.collection.JavaConversions.asScalaIterator($s.asInstanceOf[${Types.GenericArrayInterface}[Any]].iterator()).map(_elem => ${fromMutable(array.elementType)("_elem")}).toVector"
    }).getOrElse((s: String) =>
      //if (schema.getType == Schema.Type.BYTES) s"$s.asInstanceOf[java.nio.ByteBuffer].array()"
      //else s"$s.asInstanceOf[${schema.scalaTypeName}]")
      s"$s.asInstanceOf[${schema.scalaTypeName}]")
  }

  def toMutable(schema: Schema): String => String =
    schema.asOptional.map { opt => (s: String) =>
      s"$s.map(_x => ${toMutable(opt.nonNullSchema)("_x")})getOrElse(null)"
    }.orElse(schema.asUnion.map { union => (s: String) =>
      if (union.isOversize) s"${union.unionTypeExtractorName}($s)"
      else s"$s.fold(${union.unionTypeExtractorName})"
    }).orElse(schema.asOptionalUnion.map { optU => (s: String) =>
      if (optU.isOversize) s"$s.map(${optU.unionTypeExtractorName}).getOrElse(null)"
      else s"$s.map(_.fold(${optU.unionTypeExtractorName})).getOrElse(null)"
    }).orElse(schema.asMap.map { map => (s: String) =>
      s"scala.collection.JavaConversions.mapAsJavaMap($s.mapValues( _v => ${toMutable(map.valueType)("_v")}))"
    }).orElse(schema.asEnum.map { enum => (s: String) =>
      s"new org.apache.avro.generic.GenericData.EnumSymbol(${enum.getName.asSymbol}.schema, $s.toString)"
      //s"$s.toString"
    }).orElse(schema.asRecord.map { record => (s: String) =>
      s"$s.toMutable"
    }).orElse(schema.asArray.map { array => (s: String) =>
      s"scala.collection.JavaConversions.asJavaCollection($s.map(_e => ${toMutable(array.elementType)("_e")}))"
      //}).getOrElse((s: String) => if (schema.getType == Schema.Type.BYTES) s"java.nio.ByteBuffer.wrap($s)" else s)
    }).getOrElse((s: String) => s)

}

object AvroGenerator {

  val defaultNameSpace = "default.schema"
}