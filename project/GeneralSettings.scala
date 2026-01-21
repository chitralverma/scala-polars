import sbt.*
import sbt.Keys.*

import Utils.*
import sbtassembly.AssemblyPlugin.autoImport.*

object GeneralSettings {

  val scala212 = "2.12.21"
  val scala213 = "2.13.16"
  val scala33 = "3.3.7"

  val defaultScalaVersion: String = scala213
  val supportedScalaVersions: Seq[String] = Seq(scala212, scala213, scala33)

  lazy val commonSettings = Seq(
    organization := "com.github.chitralverma",
    versionScheme := Some("early-semver"),
    homepage := Some(url("https://github.com/chitralverma/scala-polars")),
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
    fork := true,
    turbo := true,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )

  lazy val settings: Seq[Setting[_]] = Seq(
    name := "scala-polars",
    nativeRoot := baseDirectory.value.toPath.resolveSibling("native").toFile
  )

}
