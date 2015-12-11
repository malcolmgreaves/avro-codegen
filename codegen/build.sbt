import com.nitro.build._

import PublishHelpers._

// GAV coordinates
lazy val projectName = "avro-codegen-compiler"
name := projectName
version := semver.toString

// dependencies & resolvers
libraryDependencies ++= Seq(
  "org.scala-lang"             %   "scala-reflect" % scalaVersion.value,
  "io.spray"                   %%  "spray-json"    % "1.3.2",
  "org.apache.avro"            %   "avro"          % "1.7.7",
  "org.scalatest"              %%  "scalatest"     % "2.2.1" % Test
)
resolvers ++= Seq(
  "Typesafe Releases Repository - common" at "http://repo.typesafe.com/typesafe/releases/"
)

// compile & runtime settings
scalaVersion := "2.10.6"
CompileScalaJava.pluginSettings(devConfig)
javaOptions := jvmOpts

// publishing
pubSettings
