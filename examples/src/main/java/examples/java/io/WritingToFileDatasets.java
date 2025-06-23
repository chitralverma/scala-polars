package examples.java.io;

import com.github.chitralverma.polars.Polars;
import com.github.chitralverma.polars.api.DataFrame;
import examples.scala.utils.CommonUtils;

/**
 * Polars supports various output file formats like the following,
 *
 * <ul>
 *   <li>{@link com.github.chitralverma.polars.api.io.Writeable#parquet(String) Apache Parquet}
 *   <li>{@link com.github.chitralverma.polars.api.io.Writeable#ipc(String) Apache IPC}
 * </ul>
 *
 * <p>A {@link DataFrame} can be written to an object storage as a file in one of the supported
 * formats mentioned above.
 *
 * <p>Since each format and storage may have its own additional options, Polars allows a simple
 * builder pattern which can be used to supply these options.
 *
 * <p>While the examples below have been provided for Parquet files only, they also similarly apply
 * on the other supported file formats.
 */
public class WritingToFileDatasets {

  public static void main(String[] args) {

    /* Read a dataset as a DataFrame lazily or eagerly */
    String path = CommonUtils.getResource("/files/web-ds/data.ipc");
    DataFrame df = Polars.scan().ipc(path).collect();

    System.out.println("Showing ipc file as a DataFrame to stdout.");
    df.show();

    System.out.printf("Total rows: %s%n%n", df.count());

    /* Write this DataFrame to local filesystem at the provided path */
    String outputPath = CommonUtils.getOutputLocation("output.pq");
    df.write().parquet(outputPath);
    System.out.printf("File written to location: %s%n%n", outputPath);

    /* Overwrite output if already exists */
    df.write().option("write_mode", "overwrite").parquet(outputPath);
    System.out.printf("File overwritten at location: %s%n%n", outputPath);

    /* Write output file with compression */
    df.write()
        .option("write_compression", "zstd")
        .option("write_mode", "overwrite")
        .option("write_parquet_stats", "full")
        .parquet(outputPath);
    System.out.printf("File overwritten at location: %s with compression%n%n", outputPath);

    /* Write output file to Amazon S3 object store */
    String s3Path = "s3://bucket/output.pq";
    df.write()
        .option("write_compression", "zstd")
        .option("write_mode", "overwrite")
        .option("write_parquet_stats", "full")
        .option("aws_default_region", "us‑east‑2")
        .option("aws_access_key_id", "ABC")
        .option("aws_secret_access_key", "XYZ")
        .parquet(s3Path);
    System.out.printf("File overwritten at location: %s with compression%n%n", s3Path);
  }
}
