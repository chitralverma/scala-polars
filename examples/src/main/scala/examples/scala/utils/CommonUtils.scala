package examples.scala.utils

import java.nio.file.{Files, Paths, StandardCopyOption}

object CommonUtils {

  def getResource(path: String): String = {
    val target =
      Files.createTempFile("tmp-resource-", s"-${Paths.get(path).getFileName.toString}")
    Files.copy(
      this.getClass.getResourceAsStream(path),
      target,
      StandardCopyOption.REPLACE_EXISTING
    )

    target.toAbsolutePath.toString
  }

  def getOutputLocation(path: String): String = {
    val target =
      Files.createTempFile("tmp-resource-", s"-${Paths.get(path).getFileName.toString}")
    Files.deleteIfExists(target)

    target.toAbsolutePath.toString
  }

}
