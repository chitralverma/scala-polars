import java.net.URI

import sbt.*
import sbt.Keys.*

import Utils.*
import sbtassembly.AssemblyPlugin.autoImport.*

object GeneralSettings {

  val scala212 = "2.12.21"
  val scala213 = "2.13.18"
  val scala33 = "3.3.8"

  val defaultScalaVersion: String = scala213
  val supportedScalaVersions: Seq[String] = Seq(scala212, scala213, scala33)

  private val jdkJavadocUrl = "https://docs.oracle.com/en/java/javase/21/docs/api/"

  private def docExternalMappingOptions(scalaVer: String): Seq[String] =
    if (scalaVer.startsWith("3.")) {
      Seq(s"-external-mappings:.*java/.*::javadoc::$jdkJavadocUrl")
    } else if (scalaVer.startsWith("2.12")) {
      Seq(
        "-doc-external-doc",
        s"/modules/java.base#$jdkJavadocUrl",
        "-no-link-warnings"
      )
    } else {
      Seq(
        "-jdk-api-doc-base",
        jdkJavadocUrl,
        "-doc-external-doc",
        s"/modules/java.base#$jdkJavadocUrl"
      )
    }

  lazy val commonSettings: Seq[Setting[?]] = Seq(
    organization := "com.github.chitralverma",
    versionScheme := Some("early-semver"),
    homepage := Some(URI.create("https://github.com/chitralverma/scala-polars")),
    licenses := Seq(License.Apache2),
    developers := List(
      Developer(
        id = "chitralverma",
        name = "Chitral Verma",
        email = "chitral.verma@gmail.com",
        url = URI.create("https://github.com/chitralverma")
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
      "-Xfatal-warnings",
      "-release",
      "17"
    ),
    fork := true,
    // Enable external API link resolution.
    autoAPIMappings := true,
    Compile / doc / scalacOptions := {
      val defaultOpts = (Compile / doc / scalacOptions).value
      val extraOpts = docExternalMappingOptions(scalaVersion.value)
      val allOpts = defaultOpts ++ extraOpts
      if (scalaVersion.value.startsWith("2.12")) {
        allOpts.filterNot(_.startsWith("-jdk-api-doc-base"))
      } else {
        allOpts
      }
    },
    Test / doc / scalacOptions := {
      val defaultOpts = (Test / doc / scalacOptions).value
      val extraOpts = docExternalMappingOptions(scalaVersion.value)
      val allOpts = defaultOpts ++ extraOpts
      if (scalaVersion.value.startsWith("2.12")) {
        allOpts.filterNot(_.startsWith("-jdk-api-doc-base"))
      } else {
        allOpts
      }
    },
    // Keep directory-based classpath to allow NativeLoader extraction.
    exportJars := false,
    // sbt 2.x eagerly closes the test ClassLoader during JVM shutdown, which prevents Jacoco's
    // shutdown hook from loading its classes and causes a `NoClassDefFoundError` on exit.
    // Keeping the ClassLoader open during JVM shutdown solves the classloading error.
    Test / closeClassLoaders := false,
    // Compile Java sources targeting JDK 17 bytecode.
    javacOptions ++= Seq("--release", "17"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )

}
