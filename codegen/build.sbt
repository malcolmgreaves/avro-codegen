import com.nitro.build._

import PublishHelpers._

sbtPlugin := true
// scripted test settings
scriptedSettings
scriptedLaunchOpts <+= version apply { v => "-Dproject.version="+v }
scriptedBufferLog := false

// GAV coordinates
lazy val projectName = "avro-codegen-compiler"
name := projectName
version := semver.toString

// dependencies & resolvers
libraryDependencies ++= Seq(
  "org.scala-lang" %  "scala-reflect" % scalaVersion.value,
  "io.spray"       %% "spray-json"    % "1.3.2",
  apacheAvroDep,
  "org.scalatest" %%  "scalatest"     % "2.2.6" % Test
)
resolvers ++= Seq(
  "Typesafe Releases Repository - common" at "http://repo.typesafe.com/typesafe/releases/"
)

// compile & runtime settings
scalaVersion := scala210v
CompileScalaJava.pluginSettings(devConfig)
javaOptions := jvmOpts

// publishing
pubSettings
