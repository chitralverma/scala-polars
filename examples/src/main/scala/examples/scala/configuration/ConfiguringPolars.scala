package examples.scala.configuration

import java.io.File
import java.nio.file.{Files, Path}

import com.github.chitralverma.polars.Polars

object ConfiguringPolars {

  def main(args: Array[String]): Unit = {

    /* Checking the version scala-polars is compiled against. */
    val version: String = Polars.version()
    printf("scala-polars has been compiled against version '%s'%n%n", version)

    /* Get default configuration. */
    printf("Default Configuration:%n%s%n%n", Polars.config)

    /* Updating configuration. */

    /* Update the number of rows shown while doing `df.show()` */
    Polars.config.update().withMaxTableRows(20).apply()
    printf("After updating number of rows:%n%s%n%n", Polars.config)

    /* Update the number of columns shown while doing `df.show()` */
    Polars.config.update().withMaxTableColumns(20).apply()
    printf("After updating number of columns:%n%s%n%n", Polars.config)

    /* Reset config */
    Polars.config.update().reset().apply()
    printf("After resetting config:%n%s%n%n", Polars.config)

    /* Chaining configuration options */
    val options = Map("POLARS_TABLE_WIDTH" -> "5000")

    Polars.config
      .update()
      .withMaxTableRows(20)
      .withMaxTableColumns(20)
      .withOption("POLARS_FMT_TABLE_CELL_ALIGNMENT", "RIGHT")
      .withOptions(options)
      .apply()

    printf("After chained configs:%n%s%n%n", Polars.config)

    /* Persisting current configuration to file */
    val tempDirectory: Path = Files.createTempDirectory("polars-config-")
    val tempFile: File =
      Files.createTempFile(tempDirectory, "temp-polars-config-", "plcfg").toFile
    Polars.config.saveTo(tempFile, overwrite = true)

    /* Reloading current configuration to file */ /* Reloading current configuration to file */
    Polars.config.update().reset().apply()
    printf("After resetting config:%n%s%n%n", Polars.config)

    Polars.config.update().fromPath(tempFile).apply()
    printf("After reloading config from file path:%n%s%n", Polars.config)

  }
}
