import com.nitro.build._

import PublishHelpers._

// GAV coordinates
lazy val projectName = "avro-codegen-e2e"
organization := ""
name         := projectName
version      := ""

// dependencies & resolvers
libraryDependencies ++= Seq(
  "com.gonitro"   %% "avro-codegen-runtime" % sys.props("project.version"),
  "com.chuusai"   %% "shapeless"            % "2.3.2",
  "org.scalatest" %% "scalatest"            % "3.0.1" % Test
)
resolvers ++= Seq(
  "Sonatype Releases"  at "https://oss.sonatype.org/content/repositories/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
)

// compile & runtime settings
scalaVersion := "2.12.3"

// special library dependency
libraryDependencies += { "org.scala-lang" % "scala-compiler" % scalaVersion.value }

// publish settings
publish := {}

// test options
fork in Test    := false
publishArtifact := false
