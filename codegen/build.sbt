import com.nitro.build._

import PublishHelpers._

ScriptedPlugin.scriptedSettings
sbtPlugin := true
// scripted test settings
scriptedLaunchOpts += {  "-Dproject.version="+version.value }
scriptedBufferLog := false

// GAV coordinates
name := "avro-codegen-compiler"
version := semver.toString

// dependencies & resolvers
libraryDependencies ++= Seq(
  "org.scala-lang" %  "scala-reflect" % scalaVersion.value,
  "io.spray"       %% "spray-json"    % "1.3.3",
  apacheAvroDep,
  "org.scalatest" %%  "scalatest"     % "3.0.1" % Test
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
