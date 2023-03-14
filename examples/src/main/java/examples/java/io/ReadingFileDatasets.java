package examples.java.io;

import examples.scala.utils.CommonUtils;
import org.polars.scala.polars.Polars;
import org.polars.scala.polars.api.DataFrame;
import org.polars.scala.polars.api.LazyFrame;

/**
 * Polars supports various input file formats like the following,
 *
 * <ul>
 *   <li>{@link Polars#csv() CSV} (delimited format like CSV, TSV, etc.)
 *   <li>{@link Polars#parquet() Apache Parquet}
 *   <li>{@link Polars#ipc() Apache Arrow IPC}
 *   <li>{@link Polars#ndJson() New line delimited JSON}
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
    DataFrame df = Polars.parquet().scan(path).collect();

    System.out.println("Showing parquet file as a DataFrame to stdout.");
    df.show();

    System.out.printf("Total rows: %s%n%n", df.count());

    /* For multiple Parquet file(s) */
    DataFrame multiLdf = Polars.parquet().read(path, path, path);

    System.out.println("Showing multiple parquet files as 1 DataFrame to stdout.");
    multiLdf.show();
    System.out.printf("Total rows: %s%n%n", multiLdf.count());

    /* Providing additional options with Parquet file input */
    DataFrame pqDfWithOpts =
        Polars.parquet()
            .lowMemory(true)
            .nRows(3)
            .cache(false)
            .rowCountColName("SerialNum")
            .scan(path)
            .collect();

    System.out.println("Showing parquet file as a DataFrame to stdout.");
    pqDfWithOpts.show();

    System.out.printf("Total rows: %s%n%n", pqDfWithOpts.count());
  }
}
