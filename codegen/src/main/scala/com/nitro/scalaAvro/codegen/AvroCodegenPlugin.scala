package com.nitro.scalaAvro.codegen

import java.io.File
import sbt.Keys._
import sbt._

object AvroCodegenPlugin extends AutoPlugin {
  import autoImport._

  override def projectSettings: Seq[Setting[_]] = avroSettings

  override def requires = plugins.JvmPlugin

  override def trigger = allRequirements

  object autoImport {

    val avroConfig = config("avro")

    lazy val genTask = TaskKey[Seq[File]]("generate", "Generate classes from avro schemas")

    lazy val generatedClassesRoot = SettingKey[File]("avroClassesSource", "Folder where the case classes will be generated")

    val inputDir = sourceDirectory := {(sourceDirectory in Compile).value} / "avro"

    val outputDir = generatedClassesRoot := {(sourceManaged in Compile).value}  / "generated_avro_classes"

    val classPath = managedClasspath := Classpaths.managedJars(avroConfig, {classpathTypes.value}, {update.value})

    lazy val avroSettings: Seq[Setting[_]] = inConfig(avroConfig)(Seq[Setting[_]](
      inputDir,
      outputDir,
      classPath,
      genTask := {
	  val out = {streams.value}
	  val srcDir = {(sourceDirectory in avroConfig).value}
   	  val targetDir = {(generatedClassesRoot in avroConfig).value}
          val cachedCompile = FileFunction.cached(
            out.cacheDirectory / "avro",
            inStyle = FilesInfo.hash,
            outStyle = FilesInfo.exists
          ) { (in: Set[File]) =>
              AvroRoot.generate(in, srcDir, targetDir, out.log)
            }
          cachedCompile((srcDir ** "*.av*").get.toSet).toSeq
      }
    )) ++ Seq[Setting[_]](
      sourceGenerators in Compile += (genTask in avroConfig),
      cleanFiles += {(generatedClassesRoot in avroConfig).value},
      ivyConfigurations += avroConfig,
      managedSourceDirectories in Compile += (generatedClassesRoot in avroConfig).value
    )
  }

}
