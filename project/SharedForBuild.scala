import sbt._
import Keys._

object SharedForBuild extends Build {

  // use sbt-dev-settings to configure

  publishArtifact in ThisBuild := false

  import com.nitro.build._
  import PublishHelpers._

  lazy val semver = SemanticVersion(0, 4, 0, isSnapshot = false)

  lazy val apacheAvroDep = "org.apache.avro" % "avro" % "1.9.2"

  private[this] def githubUrl(id: String) =
    new URL("http", "github.com", s"/$id")

  lazy val pluginDevelopers = Seq(
    Developer("pkinsky",        "Paul Kinsky",     "pkinsky@gmail.com",         githubUrl("pkinsky")),
    Developer("malcolmgreaves", "Malcolm Greaves", "greaves.malcolm@gmail.com", githubUrl("malcolmgreaves")),
    Developer("ebiggs",         "Eric Biggs",      "ebiggs@gmail.com",          new URL("http", "ebiggs.com", ""))
  )

  lazy val scala213v = "2.13.2"
  lazy val scala212v = "2.12.6"
  lazy val scala211v = "2.11.11"
  lazy val scala210v = "2.10.6"

  // ** NOTE **    We want to upgrade to Java 8 ASAP. Spark is still stuck at Java 7.
  // [JIRA Issue]  https://issues.apache.org/jira/browse/SPARK-6152

  lazy val devConfig =  {
    import CompileScalaJava._
    Config.spark.copy(scala =
      ScalaConfig(
        fatalWarnings = false,
        logImplicits = false,
        optimize = true,
        crossCompile = Seq(scala213v, scala212v, scala211v, scala210v),
        inlineWarn = true,
        genBBackend = false
      )
    )
  }

  lazy val jvmOpts = JvmRuntime.settings(devConfig.jvmVer)

  lazy val pubSettings =
    Publish.settings(
      Repository.github("malcolmgreaves", "avro-codegen"),
      pluginDevelopers,
      ArtifactInfo.sonatype(semver),
      License.apache20
    )

}
