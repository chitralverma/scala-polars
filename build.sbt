import scala.sys.process._

/*
 ***********************
 * Constants *
 ***********************
 */

val scala212 = "2.12.15"
val scala213 = "2.13.10"
val scala32 = "3.2.1"

val defaultScalaVersion = scala213
val allScalaVersions = Seq(scala212, scala213, scala32)

ThisBuild / scalaVersion := defaultScalaVersion
ThisBuild / turbo := true

def priorTo213(scalaVersion: String): Boolean =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, minor)) if minor < 13 => true
    case _ => false
  }

lazy val javaTargetSettings = Seq(
  scalacOptions ++=
    (if (priorTo213(scalaVersion.value)) Seq("-target:jvm-1.8") else Seq("-release", "8"))
)

lazy val commonSettings = Seq(
  organization := "org.polars",
  licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      id = "chitralverma",
      name = "Chitral Verma",
      email = "chitral.verma@gmail.com",
      url = url("https://github.com/chitralverma")
    )
  ),
  scalaVersion := defaultScalaVersion,
  crossScalaVersions := allScalaVersions,
  fork := true,
  scalacOptions ++= Seq(
    "-encoding",
    "utf8",
    "-deprecation",
    "-feature",
    "-language:existentials",
    "-language:implicitConversions",
    "-language:reflectiveCalls",
    "-language:higherKinds",
    "-language:postfixOps",
    "-unchecked",
    "-Xfatal-warnings"
  )
)

/*
 ***********************
 * Root Module *
 ***********************
 */

lazy val root = (project in file("."))
  .settings(crossScalaVersions := Nil)
  .settings(skipReleaseSettings: _*)
  .settings(
    addCommandAlias("cleanAll", ";cleanHeaders; clean; cleanFiles; reload"),
    addCommandAlias("genHeaders", ";cleanHeaders; javah"),
    addCommandAlias("fmtAll", ";scalafmtAll; scalafmtSbt; cargoFmt; reload"),
    addCommandAlias("fmtCheckAll", ";scalafmtCheckAll; scalafmtSbtCheck; cargoCheck")
  )
  .aggregate(core, native)

/*
 ***********************
 * Native Module *
 ***********************
 */

lazy val cargoFmt = taskKey[Unit]("Formats native module and its Cargo.toml.")
lazy val cargoCheck = taskKey[Unit]("Checks the formatting of native module and its Cargo.toml.")

lazy val native = project
  .in(file("native"))
  .settings(commonSettings: _*)
  .settings(javaTargetSettings: _*)
  .settings(
    name := "scala-polars-native",
    crossPaths := false,
    nativeCompile / sourceDirectory := baseDirectory.value
  )
  .settings(
    cargoFmt := {
      val processLogger = ProcessLogger(
        (o: String) => sLog.value.info(o),
        (e: String) => sLog.value.error(e)
      )
      s"cargo fix --verbose --manifest-path ${baseDirectory.value}/Cargo.toml" ! processLogger
      s"cargo fmt --verbose --all --manifest-path ${baseDirectory.value}/Cargo.toml" ! processLogger
      s"cargo sort ${baseDirectory.value}" ! processLogger
    },
    cargoCheck := {
      val processLogger = ProcessLogger(
        (o: String) => sLog.value.info(o),
        (e: String) => sLog.value.error(e)
      )

      s"cargo fmt --check --all --manifest-path ${baseDirectory.value}/Cargo.toml" ! processLogger
      s"cargo sort --check ${baseDirectory.value}" ! processLogger
    }
  )
  .enablePlugins(JniNative)

/*
 ***********************
 * Core Module *
 ***********************
 */

lazy val cleanHeaders = taskKey[Unit]("Removes all previously generated headers")

lazy val core = project
  .in(file("core"))
  .settings(commonSettings: _*)
  .settings(javaTargetSettings: _*)
  .settings(name := "scala-polars")
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.9.0",
      "org.json4s" %% "json4s-native" % "4.0.6"
    )
  )
  .settings(
    libraryDependencies ++= {
      if (!priorTo213(scalaVersion.value))
        Seq("org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4")
      else
        Seq()
    }
  )
  .settings(
    sbtJniCoreScope := Compile,
    classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
  )
  .settings(
    assembly / mainClass := None,
    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
    }
  )
  .settings(
    cleanHeaders := {
      import scala.reflect.io.Directory

      val headerDir = (javah / target).value
      val directory = new Directory(headerDir)

      directory.deleteRecursively()
      sLog.value.info(s"Removed headers directory $headerDir")
    }
  )
  .dependsOn(native % Runtime)

/*
 ***********************
 * Release Settings *
 ***********************
 */

lazy val skipReleaseSettings = Seq(
  publishArtifact := false,
  publish / skip := true
)
