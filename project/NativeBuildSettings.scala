import java.nio.file.*

import sbt.*
import sbt.Keys.*

import scala.collection.JavaConverters.*

object NativeBuildSettings {

  lazy val managedNativeLibraries = taskKey[Seq[Path]](
    "Maps locally built, platform-dependent libraries to their locations on the classpath."
  )

  lazy val settings: Seq[Setting[_]] = Seq(
    managedNativeLibraries := Def.task {
      val logger: Logger = sLog.value
      val nativeLibsDir = target.value.toPath.resolve("native-libs")

      val managedLibs = if (Files.exists(nativeLibsDir)) {
        Files
          .find(
            nativeLibsDir,
            Int.MaxValue,
            (filePath, _) => filePath.toFile.isFile
          )
          .iterator()
          .asScala
          .toSeq
      } else {
        Seq.empty[Path]
      }

      val externalNativeLibs = sys.env.get("NATIVE_LIB_LOCATION") match {
        case Some(path) =>
          val externalPath = Paths.get(path)
          if (Files.exists(externalPath)) {
            Files
              .find(
                externalPath,
                Int.MaxValue,
                (filePath, _) => filePath.toFile.isFile
              )
              .iterator()
              .asScala
              .toSeq
          } else {
            Seq.empty[Path]
          }

        case None => Seq.empty[Path]
      }

      val allLibs = (managedLibs ++ externalNativeLibs).distinct.map(_.toAbsolutePath)

      if (allLibs.isEmpty) {
        logger.warn(
          s"Native libraries directory $nativeLibsDir does not exist and NATIVE_LIB_LOCATION is not set. " +
            "Run 'just build-native' first."
        )
      }

      allLibs
    }.value,
    resourceGenerators += Def.task {
      managedNativeLibraries.value
        .map { path =>
          val pathStr = path.toString
          val arch = path.getParent.getFileName.toString

          val libraryFile = path.toFile
          val resource = resourceManaged.value / "native" / arch / libraryFile.getName

          if (libraryFile.isDirectory) IO.copyDirectory(libraryFile, resource)
          else IO.copyFile(libraryFile, resource)

          sLog.value.success(
            s"Added resource from location '$pathStr' " +
              s"(size: ${libraryFile.length() / (1024 * 1024)} MBs) to classpath."
          )

          resource
        }
    }.taskValue,

    // Exclude native libs from sources.jar
    Compile / packageSrc / mappings := {
      val nativeDir =
        (Compile / resourceManaged).value.toPath.resolve("native").toFile.getAbsolutePath
      val original = (Compile / packageSrc / mappings).value
      original.filterNot { case (file, _) =>
        file.getAbsolutePath.startsWith(nativeDir)
      }
    },

    // Exclude native libs from javadoc.jar
    Compile / packageDoc / mappings := {
      val nativeDir =
        (Compile / resourceManaged).value.toPath.resolve("native").toFile.getAbsolutePath
      val original = (Compile / packageDoc / mappings).value
      original.filterNot { case (file, _) =>
        file.getAbsolutePath.startsWith(nativeDir)
      }
    }
  )

}
