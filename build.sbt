
organization in ThisBuild := "io.malcolmgreaves"

scalaVersion in ThisBuild := scala212v

lazy val root =
  project.in(file("."))
    .settings {
      publishArtifact := false
      publish := { } // prevents us from publishing a root artifact (do not remove!!)
      publishLocal := { }
    }
    .aggregate(runtime, codegen)

lazy val runtime = project.in(file("runtime"))

lazy val codegen = project.in(file("codegen"))

lazy val ShortTest = config("short") extend Test
