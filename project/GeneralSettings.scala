import sbt.Keys._
import sbt._

import Utils._

object GeneralSettings {

  val scala212 = "2.12.17"
  val scala213 = "2.13.10"
  val scala32 = "3.2.1"

  val defaultScalaVersion: String = scala213
  val supportedScalaVersions: Seq[String] = Seq(scala212, scala213)

  lazy val settings: Seq[Setting[_]] = Seq(
    organization := "org.polars",
    name := "scala-polars",
    version := "0.1.0-SNAPSHOT",
    versionScheme := Some("early-semver"),
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
    crossScalaVersions := supportedScalaVersions,
    fork := true,
    turbo := true,
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
    ) ++ (if (priorTo213(scalaVersion.value)) Seq("-target:jvm-1.8")
          else Seq("-release", "8")),
    nativeRoot := baseDirectory.value.toPath.resolveSibling("native").toFile
  )

}
