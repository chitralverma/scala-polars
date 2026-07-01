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

  /** JDK 8 install for the forked test JVM. sbt 2.x runs on JDK 17+, so JDK 8 can no longer host
    * the build; instead the test JVM is forked onto a JDK 8 to prove the `-release 8` artifacts
    * run on the binary-compatibility floor. CI sets `JAVA_HOME_8` (see release.yml); the
    * arch-specific `JAVA_HOME_8_X64` / `JAVA_HOME_8_AARCH64` from actions/setup-java are honoured
    * as fallbacks. Absent locally, tests run on the ambient JVM.
    */
  private val jdk8TestHome: Option[File] =
    Seq("JAVA_HOME_8", "JAVA_HOME_8_X64", "JAVA_HOME_8_ARM64", "JAVA_HOME_8_AARCH64").iterator
      .flatMap(sys.env.get)
      .map(file)
      .find(_.exists())

  private val jdkJavadocUrl = "https://docs.oracle.com/en/java/javase/21/docs/api/"

  private def docExternalMappingOptions(scalaVer: String): Seq[String] =
    if (scalaVer.startsWith("3.")) {
      // Scala 3 scaladoc. Comma-separates multiple mappings.
      Seq(s"-external-mappings:.*java/.*::javadoc::$jdkJavadocUrl")
    } else if (scalaVer.startsWith("2.12")) {
      // Scala 2.12 scaladoc (lacks -jdk-api-doc-base, supported in 2.13+)
      Seq(
        "-doc-external-doc",
        s"/modules/java.base#$jdkJavadocUrl",
        "-no-link-warnings"
      )
    } else {
      // Scala 2.13 scaladoc:
      //  * -jdk-api-doc-base sets the base for the java.* linker.
      //  * -doc-external-doc keys on classpath entry canonical paths.
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
      "-Xfatal-warnings"
    ) ++ (if (priorTo213(scalaVersion.value)) Seq("-target:jvm-1.8")
          else Seq("-release", "8")),
    fork := true,
    // Let scaladoc/javadoc resolve external `[[scala.*]]` (and other dependency) doc links from the
    // `apiURL` published in each dependency's POM, instead of failing the fatal doc build. Replaces
    // the former sbt-api-mappings plugin.
    autoAPIMappings := true,
    Compile / doc / scalacOptions := {
      val oldOpts = (Compile / doc / scalacOptions).value
      oldOpts ++ docExternalMappingOptions(scalaVersion.value)
    },
    Test / doc / scalacOptions := {
      val oldOpts = (Test / doc / scalacOptions).value
      oldOpts ++ docExternalMappingOptions(scalaVersion.value)
    },
    // sbt 2.x defaults exportJars := true, which routes classpaths through jars and breaks
    // `getResource("/")` / `resource.toURI` — NativeLoader relies on those to extract the bundled
    // native library from the classpath. Keep the directory-based classpath.
    exportJars := false,
    Test / javaHome := jdk8TestHome,
    // sbt 2.x caches tasks globally based on inputs. However, `sbt-scoverage`'s compiler plugin
    // instrumentation changes the output but doesn't correctly invalidate sbt's global cache,
    // leading to global cache hits on uninstrumented classes. Override `localCacheDirectory`
    // when coverage is enabled to bypass the global cache and force clean instrumentation.
    localCacheDirectory := {
      val defaultCache = localCacheDirectory.value
      val covEnabled = (ThisBuild / scoverage.ScoverageKeys.coverageEnabled).value
      if (covEnabled) {
        target.value / "sbt-cache"
      } else {
        defaultCache
      }
    },
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @_*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )

}
