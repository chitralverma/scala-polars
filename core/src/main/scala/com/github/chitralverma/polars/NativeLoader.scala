package com.github.chitralverma.polars

import java.nio.file._

class NativeLoader(nativeLibrary: String) {
  NativeLoader.load(nativeLibrary)
}

object NativeLoader {
  def load(nativeLibrary: String): Unit = {
    def loadPackaged(arch: String): Unit = {
      val lib: String = System.mapLibraryName(nativeLibrary)
      val resourcePath: String = s"/native/$arch/$lib"

      val resourceStream = Option(
        this.getClass.getResourceAsStream(resourcePath)
      ) match {
        case Some(s) => s
        case None =>
          throw new UnsatisfiedLinkError(
            s"Native library $lib ($resourcePath) cannot be found on the classpath."
          )
      }

      val tmp: Path = Files.createTempDirectory("jni-")
      val extractedPath = tmp.resolve(lib)

      try
        Files.copy(resourceStream, extractedPath)
      catch {
        case ex: Exception =>
          throw new UnsatisfiedLinkError(
            s"Error while extracting native library:\n$ex"
          )
      }

      System.load(extractedPath.toAbsolutePath.toString)
    }

    def load(): Unit = try
      System.loadLibrary(nativeLibrary)
    catch {
      case e: Throwable =>
        val arch = System.getProperty("os.arch").toLowerCase(java.util.Locale.ROOT) match {
          case "aarch64" | "arm64" => "aarch64"
          case "amd64" | "x86_64" => "x86_64"
          case a => a
        }

        try
          loadPackaged(arch)
        catch {
          case t: Throwable =>
            t.addSuppressed(e)
            throw new IllegalStateException(
              s"Unable to load the provided native library '$nativeLibrary' for architecture '$arch'.",
              t
            )
        }
    }

    load()
  }
}
