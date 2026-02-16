import java.nio.file.*

import sbt.*
import sbt.Keys.*

import scala.collection.JavaConverters.*

import Utils.*

object NativeBuildSettings {

  lazy val managedNativeLibraries = taskKey[Seq[Path]](
    "Maps locally built, platform-dependent libraries to their locations on the classpath."
  )

  lazy val settings: Seq[Setting[_]] = Seq(
    managedNativeLibraries := Def.task {
      val logger: Logger = sLog.value
      val nativeLibsDir = target.value.toPath.resolve("native-libs")

      if (Files.exists(nativeLibsDir)) {
        Files
          .find(
            nativeLibsDir,
            Int.MaxValue,
            (filePath, _) => filePath.toFile.isFile
          )
          .iterator()
          .asScala
          .toSeq
          .map(_.toAbsolutePath)
      } else {
        logger.warn(
          s"Native libraries directory $nativeLibsDir does not exist. Run 'just build-native' first."
        )
        Seq.empty[Path]
      }
    }.value,
    resourceGenerators += Def.task {
      managedNativeLibraries.value
        .map { path =>
          val pathStr = path.toString
          val arch = path.getParent.getFileName.toString

          val libraryFile = path.toFile
          val resource = resourceManaged.value / "native" / arch / libraryFile.getName

          IO.copyDirectory(libraryFile, resource)

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
