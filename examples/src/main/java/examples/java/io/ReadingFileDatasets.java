package examples.java.io;

import com.github.chitralverma.polars.Polars;
import com.github.chitralverma.polars.api.DataFrame;
import com.github.chitralverma.polars.api.LazyFrame;
import examples.scala.utils.CommonUtils;

/**
 * Polars supports various input file formats like the following,
 *
 * <ul>
 *   <li>{@link com.github.chitralverma.polars.api.io.Scannable#csv CSV} (delimited
 *       format like CSV, TSV, etc.)
 *   <li>{@link com.github.chitralverma.polars.api.io.Scannable#parquet Apache Parquet}
 *   <li>{@link com.github.chitralverma.polars.api.io.Scannable#ipc Apache Arrow IPC}
 *   <li>{@link com.github.chitralverma.polars.api.io.Scannable#jsonLines New line
 *       delimited JSON}
 * </ul>
 *
 * <p>All the above formats are compatible with the lazy or eager input API and users can supply 1
 * or more file paths which will be read in parallel to return a {@link LazyFrame} or a {@link
 * DataFrame}.
 *
 * <p>Since each format may have its own additional options (example: delimiter for CSV format),
 * Polars allows a simple builder pattern which can be used to supply these options.
 *
 * <p>While the examples below have been provided for Parquet files only, they also similarly apply
 * on the other supported file formats.
 *
 * <p>Some additional examples may also be found in {@link LazyAndEagerAPI}.
 */
public class ReadingFileDatasets {

  public static void main(String[] args) {

    /* For one Parquet file */
    String path = CommonUtils.getResource("/files/web-ds/data.parquet");
    DataFrame df = Polars.scan().parquet(path).collect();

    System.out.println("Showing parquet file as a DataFrame to stdout.");
    df.show();

    System.out.printf("Total rows: %s%n%n", df.count());

    /* For multiple Parquet file(s) */
    DataFrame multiLdf = Polars.scan().parquet(path, path, path).collect();

    System.out.println("Showing multiple parquet files as 1 DataFrame to stdout.");
    multiLdf.show();
    System.out.printf("Total rows: %s%n%n", multiLdf.count());

    /* Providing additional options with Parquet file input */
    DataFrame pqDfWithOpts =
        Polars.scan()
            .option("scan_parquet_low_memory", "true")
            .option("scan_parquet_n_rows", "3")
            .option("scan_parquet_cache", "false")
            .option("scan_parquet_row_index_name", "SerialNum")
            .parquet(path)
            .collect();

    System.out.println("Showing parquet file as a DataFrame to stdout.");
    pqDfWithOpts.show();

    System.out.printf("Total rows: %s%n%n", pqDfWithOpts.count());
  }
}
