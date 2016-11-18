package com.nitro.scalaAvro.codegen

import java.io.File
import java.nio.file.{ Paths, Files }
import java.nio.charset.StandardCharsets

import sbt.Logger

object AvroRoot {

  def generatedCodeHeader(fname: String)(printer: FunctionalPrinter): FunctionalPrinter = {
    val fnameUpper = fname.map(_.toUpper)
    printer.addM(
      s"""/**
         |Code generated from avro schemas by scalaAvro. Do not modify.
         |"ALL THESE FILES ARE YOURS—EXCEPT $fnameUpper / ATTEMPT NO MODIFICATIONS THERE"
                                                         |*/
                                                         |"""
    )
  }

  def generate(schemaFiles: Iterable[File], src: File, dest: File, log: Logger): Set[File] = {
    log.info(s"running on files: "+schemaFiles.map(_.getAbsolutePath).mkString(","))

    dest.mkdirs()

    val input = SchemaParser.getSchemas(schemaFiles)

    val gen = new AvroGenerator()
    val generatedFiles = input.map {
      case (namespace, Right(enum)) =>
        val fname = enum.getName+".scala"
        (namespace, fname, FunctionalPrinter()
          .add(s"package $namespace")
          .call(generatedCodeHeader(fname))
          .call(gen.printEnum(enum)))
      case (namespace, Left(record)) =>
        val fname = record.getName+".scala"
        (namespace, fname, FunctionalPrinter()
          .add(s"package $namespace")
          .call(generatedCodeHeader(fname))
          .call(gen.printRecord(record)))
    }.map {
      case (namespace, name, printer) =>
        val namespacePath = namespace.split("\\.").mkString(File.separator)
        val fileDest = new File(dest.getAbsolutePath + File.separator + namespacePath)
        fileDest.mkdirs()
        val path = Paths.get(fileDest.getAbsolutePath, name)
        val bytes = printer.result().getBytes(StandardCharsets.UTF_8)
        log.info(s"writing output for name: $name to $path")
        Files.write(path, bytes)
        path.toFile
    }.toSet
    log.debug(s"""Generated files: ${generatedFiles.mkString(",")}""")
    generatedFiles
  }
}
