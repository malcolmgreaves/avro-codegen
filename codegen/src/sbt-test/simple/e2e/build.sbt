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
  "com.chuusai"   %% "shapeless"            % "2.2.3",
  "org.scalatest" %% "scalatest"            % "2.2.1" % Test
)
resolvers ++= Seq(
  "Sonatype Releases"  at "https://oss.sonatype.org/content/repositories/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
)

// compile & runtime settings
scalaVersion := "2.11.7"
CompileScalaJava.librarySettings {
  import CompileScalaJava._
  Config.spark.copy(scala =
    ScalaConfig(
      fatalWarnings = false,
      logImplicits  = false,
      optimize      = true,
      crossCompile  = Seq("2.11.7", "2.10.6")
    )
  )
}

// special library dependency
libraryDependencies <+= (scalaVersion) { v => "org.scala-lang" % "scala-compiler" % v }


// publish settings
publish := {}

// test options
fork in Test    := false
publishArtifact := false
