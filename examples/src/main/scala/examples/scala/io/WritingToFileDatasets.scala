package examples.scala.io

import org.polars.scala.polars.Polars
import org.polars.scala.polars.api.io.{WriteCompressions, WriteModes}

import examples.scala.utils.CommonUtils

/** Polars supports various output file formats like the following,
  *   - [[org.polars.scala.polars.api.io.Writeable.parquet Apache Parquet]]
  *   - [[org.polars.scala.polars.api.io.Writeable.ipc Apache Arrow IPC]]
  *   - [[org.polars.scala.polars.api.io.Writeable.avro Apache Avro]]
  *
  * A [[org.polars.scala.polars.api.DataFrame DataFrame]] can be written to an object storage as a
  * file in one of the supported formats mentioned above.
  *
  * Since each format and storage may have its own additional options, Polars allows a simple
  * builder pattern which can be used to supply these options.
  *
  * While the examples below have been provided for Parquet files only, they also similarly apply
  * on the other supported file formats.
  */
object WritingToFileDatasets {

  def main(args: Array[String]): Unit = {

    /* Read a dataset as a DataFrame lazily or eagerly */
    val path = CommonUtils.getResource("/files/web-ds/data.ipc")
    val df = Polars.ipc.read(path)

    println("Showing ipc file as a DataFrame to stdout.")
    df.show()

    printf("Total rows: %s%n%n", df.count())

    /* Write this DataFrame to local filesystem at the provided path */
    val outputPath = CommonUtils.getOutputLocation("output.pq")
    df.write().parquet(outputPath)
    printf("File written to location: %s%n%n", outputPath)

    /* Overwrite output if already exists */
    df.write().mode("overwrite").parquet(outputPath)
    printf("File overwritten at location: %s%n%n", outputPath)

    /* Write output file with compression */
    df.write()
      .compression(WriteCompressions.zstd, Some.apply(14))
      .mode(WriteModes.Overwrite)
      .parquet(outputPath, writeStats = true)
    printf("File overwritten at location: %s with compression%n%n", outputPath)

    /* Write output file to Amazon S3 object store */
    val s3Path: String = "s3://bucket/output.pq"
    df.write()
      .options(
        Map(
          "aws_default_region" -> "us‑east‑2",
          "aws_access_key_id" -> "ABC",
          "aws_secret_access_key" -> "XYZ"
        )
      )
      .compression(WriteCompressions.zstd, Some.apply(14))
      .mode(WriteModes.Overwrite)
      .parquet(s3Path, writeStats = true)
    printf("File overwritten at location: %s with compression%n%n", s3Path)
  }

}
