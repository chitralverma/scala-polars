package examples.java.io;

import examples.scala.utils.CommonUtils;
import org.polars.scala.polars.Polars;
import org.polars.scala.polars.api.DataFrame;
import org.polars.scala.polars.api.io.WriteCompressions;
import org.polars.scala.polars.api.io.WriteModes;
import scala.Some;

/**
 * Polars supports various output file formats like the following,
 * <ul>
 *  <li>{@link org.polars.scala.polars.api.io.Writeable#parquet(String, boolean) Apache Parquet}</li>
 *  <li>{@link org.polars.scala.polars.api.io.Writeable#ipc(String) Apache Arrow IPC}</li>
 * </ul>
 * <p>
 * A {@link DataFrame} can be written to an object storage as a file in one of the supported
 * formats mentioned above.
 * <p>
 * Since each format and storage may have its own additional options, Polars allows a simple
 * builder pattern which can be used to supply these options.
 * <p>
 * While the examples below have been provided for Parquet files only, they also similarly apply
 * on the other supported file formats.
 */

public class WritingToFileDatasets {

    public static void main(String[] args) {

        /* Read a dataset as a DataFrame lazily or eagerly */
        String path = CommonUtils.getResource("/files/web-ds/data.ipc");
        DataFrame df = Polars.ipc().read(path);

        System.out.println("Showing ipc file as a DataFrame to stdout.");
        df.show();

        System.out.printf("Total rows: %s%n%n", df.count());

        /* Write this DataFrame to local filesystem at the provided path */
        String outputPath = CommonUtils.getOutputLocation("output.pq");
        df.write().parquet(outputPath, false);
        System.out.printf("File written to location: %s%n%n", outputPath);

        /* Overwrite output if already exists */
        df.write().mode("overwrite").parquet(outputPath, false);
        System.out.printf("File overwritten at location: %s%n%n", outputPath);

        /* Write output file with compression */
        df.write()
                .compression(WriteCompressions.zstd(), Some.apply(14))
                .mode(WriteModes.Overwrite())
                .parquet(outputPath, true);
        System.out.printf("File overwritten at location: %s with compression%n%n", outputPath);

        /* Write output file to Amazon S3 object store */
        String s3Path = "s3://bucket/output.pq";
        df.write()
                .withOption("aws_default_region", "us‑east‑2")
                .withOption("aws_access_key_id", "ABC")
                .withOption("aws_secret_access_key", "XYZ")
                .compression(WriteCompressions.zstd(), Some.apply(14))
                .mode(WriteModes.Overwrite())
                .parquet(s3Path, true);
        System.out.printf("File overwritten at location: %s with compression%n%n", s3Path);

    }
}
