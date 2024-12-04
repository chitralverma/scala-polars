import java.nio.file.*

import sbt.*
import sbt.Keys.*

import scala.collection.JavaConverters.*
import scala.sys.process.*

import Utils.*

object NativeBuildSettings {

  lazy val generateNativeLibrary = taskKey[Unit](
    "Generates native library using Cargo which can be added as managed resource to classpath."
  )

  lazy val managedNativeLibraries = taskKey[Seq[Path]](
    "Maps locally built, platform-dependant libraries to their locations on the classpath."
  )

  lazy val settings: Seq[Setting[_]] = Seq(
    generateNativeLibrary := Def
      .taskDyn[Unit] {
        Def.task {
          val logger: Logger = sLog.value

          sys.env.get("SKIP_NATIVE_GENERATION") match {
            case None =>
              val processLogger = getProcessLogger(sLog.value, infoOnly = true)

              val targetTriple = sys.env.getOrElse(
                "TARGET_TRIPLE", {
                  logger.warn(
                    "Environment variable TARGET_TRIPLE was not set, getting value from `rustc`."
                  )

                  s"rustc -vV".!!.split("\n")
                    .map(_.trim)
                    .find(_.startsWith("host"))
                    .map(_.split(" ")(1).trim)
                    .getOrElse(throw new IllegalStateException("No target triple found."))
                }
              )

              val arch = targetTriple.toLowerCase(java.util.Locale.ROOT).split("-").head

              val nativeOutputDir = resourceManaged.value.toPath.resolve(s"native/$arch/")
              val cargoTomlPath = s"${baseDirectory.value.getParent}/native/Cargo.toml"

              // Build native project using cargo
              val cmd =
                s"""cargo build
                   |-Z unstable-options
                   |--release
                   |--lib
                   |--target $targetTriple
                   |--artifact-dir $nativeOutputDir""".stripMargin.replaceAll("\n", " ")

              executeProcess(cmd = cmd, cwd = Some(nativeRoot.value), sLog.value, infoOnly = true)
              logger.success(s"Successfully built native library at location '$nativeOutputDir'")

              sys.env.get("NATIVE_LIB_LOCATION") match {
                case Some(path) =>
                  val dest = Paths.get(path, arch).toAbsolutePath
                  logger.info(
                    "Environment variable NATIVE_LIB_LOCATION is set, " +
                      s"copying built native library from location '$nativeOutputDir' to '$dest'."
                  )

                  IO.copyDirectory(nativeOutputDir.toFile, dest.toFile)

                case None =>
              }

            case Some(_) =>
              logger.info(
                "Environment variable SKIP_NATIVE_GENERATION is set, skipping cargo build."
              )
          }
        }
      }
      .value,
    managedNativeLibraries := Def
      .taskDyn[Seq[Path]] {
        Def.task {
          val managedLibs = sys.env.get("SKIP_NATIVE_GENERATION") match {
            case None =>
              Files
                .find(
                  resourceManaged.value.toPath.resolve("native/"),
                  Int.MaxValue,
                  (filePath, _) => filePath.toFile.isFile
                )
                .iterator()
                .asScala
                .toSeq

            case Some(_) => Seq.empty[Path]
          }

          val externalNativeLibs = sys.env.get("NATIVE_LIB_LOCATION") match {
            case Some(path) =>
              Files
                .find(
                  Paths.get(path),
                  Int.MaxValue,
                  (filePath, _) => filePath.toFile.isFile
                )
                .iterator()
                .asScala
                .toSeq

            case None => Seq.empty[Path]
          }

          // Collect paths of built resources to later include in classpath
          (managedLibs ++ externalNativeLibs).distinct.map(_.toAbsolutePath)
        }
      }
      .dependsOn(generateNativeLibrary)
      .value,
    resourceGenerators += Def.task {
      // Add all generated resources to manage resources' classpath
      managedNativeLibraries.value
        .map { path =>
          val pathStr = path.toString
          val arch = path.getParent.getFileName.toString

          val libraryFile = path.toFile

          // native library as a managed resource file
          val resource = resourceManaged.value / "native" / arch / libraryFile.getName

          // copy native library to a managed resource, so that it is always available
          // on the classpath, even when not packaged as a jar
          IO.copyDirectory(libraryFile, resource)

          sLog.value.success(
            s"Added resource from location '$pathStr' " +
              s"(size: ${libraryFile.length() / (1024 * 1024)} MBs) to classpath."
          )

          resource
        }
    }.taskValue
  )

}
