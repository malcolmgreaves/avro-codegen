import sbt._
import Keys._

object SharedForBuild extends Build {

  // use sbt-dev-settings to configure

  publishArtifact in ThisBuild := false

  import com.nitro.build._
  import PublishHelpers._

  lazy val semver = SemanticVersion(0, 3, 2, isSnapshot = false)

  lazy val apacheAvro = "org.apache.avro" % "avro" % "1.8.1"

  private[this] def githubUrl(id: String) = 
    new URL("http", "github.com", s"/$id")

  lazy val pluginDevelopers = Seq(
    Developer("pkinsky",        "Paul Kinsky",     "pkinsky@gmail.com",         githubUrl("pkinsky")),
    Developer("malcolmgreaves", "Malcolm Greaves", "greaves.malcolm@gmail.com", githubUrl("malcolmgreaves")),
    Developer("ebiggs",         "Eric Biggs",      "ebiggs@gmail.com",          new URL("http", "ebiggs.com", ""))
  )

  lazy val devConfig =  {
    import CompileScalaJava._
    Config.spark.copy(scala = 
      ScalaConfig(
        fatalWarnings = false,
        logImplicits = false,
        optimize = true,
        crossCompile = Seq("2.11.8", "2.10.6"),
        inlineWarn = true
      )
    )
  }

  lazy val jvmOpts = JvmRuntime.settings(devConfig.jvmVer)

  lazy val pubSettings =
    Publish.settings(
      Repository.github("Nitro", "avro-codegen"),
      pluginDevelopers,
      ArtifactInfo.sonatype(semver),
      License.apache20
    )

}
