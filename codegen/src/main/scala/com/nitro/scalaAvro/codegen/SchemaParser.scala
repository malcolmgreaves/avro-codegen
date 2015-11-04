package com.nitro.scalaAvro.codegen

import org.apache.avro.Schema.Parser
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{ GenericDatumReader, GenericRecord }
import java.io.File
import RichAvro._

import collection.JavaConversions._
import collection.JavaConverters._

object SchemaParser {
  //map package name to schema
  def getSchemas(files: Iterable[File]): Seq[(String, Either[AvroRecord, AvroEnum])] = {
    val parser = new Parser()
    val schemas = files.filter(_.getName.endsWith("avsc")).map(parser.parse)

    //todo: (enhancement: track structure, gen namespaces based on parent if namespace not present)

    case class AvroSchema(nameSpace: String, name: String, schema: Either[AvroRecord, AvroEnum])

    def flatten(schemas: Seq[Schema]): Seq[AvroSchema] = {

      var seen = Set.empty[String]
  
      def inner(schema: Schema): Seq[AvroSchema] = {

        schema.asUnion.map { union => //todo: won't match if null is a union member
          schema.getTypes.flatMap(inner)
        }.orElse(schema.asOptionalUnion.map { optUnion =>
          optUnion.types.flatMap(inner)
        }).orElse(schema.asArray.map { array =>
          inner(array.elementType)
        }).orElse(schema.asMap.map { map =>
          inner(map.valueType)
        }).orElse(schema.asOptional.map { opt =>
          inner(opt.nonNullSchema)
        }).orElse(schema.asEnum.map { enum =>
          Seq(AvroSchema(enum.nameSpace, enum.getName, Right(enum)))
        }).orElse(schema.asRecord.map { record =>
          //hacky hacks to keep from defining two versions of the same schema
          if (!seen(record.upperScalaName /*.toUpperCase*/ )) {
            seen = seen + record.upperScalaName //.toUpperCase
            Seq(AvroSchema(record.nameSpace, record.upperScalaName, Left(record))) ++
              record.fields.flatMap(f => inner(f.schema))
          } else Seq.empty
        }).orElse(schema.asAnyUnion.map { anyUnion =>
          anyUnion.types.flatMap(inner)
        }
        ).getOrElse(Seq.empty)
      }

      schemas.flatMap(inner)
    }

    val flat = flatten(schemas.toSeq)
    val grouped = flat.groupBy(as => (as.nameSpace, as.name))
    val res = grouped.toSeq.map {
      case ((namespace, name), s) =>
        //todo: revisit, currently taking first schema with a given name.
        // head is safe because of groupBy, but should check all schemas with the same name are the same
        (namespace, s.head.schema)
    }
    println("done parsing schemas")
    res.collect { case (_, Left(x)) => x }.foreach(r => println(s"\t(${r.upperScalaName})"))

    res
  }
}
