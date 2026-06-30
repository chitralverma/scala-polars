package com.github.chitralverma.polars

import java.io.InputStream
import java.nio.file._
import java.security.MessageDigest
import java.util.Locale

class NativeLoader(nativeLibrary: String) {
  NativeLoader.load(nativeLibrary)
}

object NativeLoader {

  /** Normalizes a JVM `os.arch` value to the architecture directory name used when the native
    * libraries are bundled (`/native/<arch>/...`).
    */
  private[polars] def normalizeArch(osArch: String): String =
    osArch.toLowerCase(Locale.ROOT) match {
      case "aarch64" | "arm64" => "aarch64"
      case "amd64" | "x86_64" => "x86_64"
      case other => other
    }

  /** Classpath resource path of the bundled native library for `arch`. */
  private[polars] def resourcePath(nativeLibrary: String, arch: String): String =
    s"/native/$arch/${System.mapLibraryName(nativeLibrary)}"

  /** Reads `stream` fully into a byte array, always closing it. */
  private def readAll(stream: InputStream): Array[Byte] =
    try {
      val buffer = new java.io.ByteArrayOutputStream()
      val chunk = new Array[Byte](64 * 1024)
      var read = stream.read(chunk)
      while (read != -1) {
        buffer.write(chunk, 0, read)
        read = stream.read(chunk)
      }
      buffer.toByteArray
    } finally
      stream.close()

  private def shortHash(bytes: Array[Byte]): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(bytes)
      .take(8)
      .map(b => f"$b%02x")
      .mkString

  /** Extracts `bytes` to a content-addressed cache file and returns its path.
    *
    * The cache path is derived from the content hash, so concurrent processes and repeated JVM
    * starts reuse a single extracted copy instead of re-writing the (large) library every time.
    * Writing goes through a unique temp file followed by an atomic move so a partially written
    * file is never observed.
    */
  private[polars] def extractToCache(libFileName: String, bytes: Array[Byte]): Path = {
    val cacheDir = Paths
      .get(System.getProperty("java.io.tmpdir"), "scala-polars-native", shortHash(bytes))
    Files.createDirectories(cacheDir)

    val target = cacheDir.resolve(libFileName)
    if (Files.isRegularFile(target) && Files.size(target) == bytes.length.toLong)
      return target

    val tmp = Files.createTempFile(cacheDir, libFileName, ".tmp")
    try {
      Files.write(tmp, bytes)
      try
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE)
      catch {
        case _: Exception =>
          Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING)
      }
    } finally
      Files.deleteIfExists(tmp)

    target
  }

  def load(nativeLibrary: String): Unit = {
    def loadPackaged(arch: String): Unit = {
      val path = resourcePath(nativeLibrary, arch)
      val stream = Option(this.getClass.getResourceAsStream(path)).getOrElse(
        throw new UnsatisfiedLinkError(
          s"Native library (${resourcePath(nativeLibrary, arch)}) cannot be found on the classpath."
        )
      )

      val extracted =
        try
          extractToCache(System.mapLibraryName(nativeLibrary), readAll(stream))
        catch {
          case ex: Exception =>
            throw new UnsatisfiedLinkError(s"Error while extracting native library:\n$ex")
        }

      try
        System.load(extracted.toAbsolutePath.toString)
      catch {
        case ex: Throwable =>
          throw new UnsatisfiedLinkError(
            s"Error while loading extracted native library '$extracted':\n$ex"
          )
      }
    }

    def load(): Unit = try
      System.loadLibrary(nativeLibrary)
    catch {
      case e: Throwable =>
        val arch = normalizeArch(System.getProperty("os.arch"))

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
