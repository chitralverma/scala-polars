package examples.java.io;

import com.github.chitralverma.polars.Polars;
import com.github.chitralverma.polars.api.DataFrame;
import com.github.chitralverma.polars.api.LazyFrame;
import com.github.chitralverma.polars.api.Row;
import examples.scala.utils.CommonUtils;
import scala.collection.Iterator;

/**
 * Polars provides 2 API for reading datasets lazily ({@code scan}) or eagerly ({@code read}).
 *
 * <p>These APIs serve different purposes and result in either a {@link LazyFrame} or a {@link
 * DataFrame}. A LazyFrame can be materialized to a DataFrame and vice-versa if required.
 *
 * <p>
 *
 * <h2>Lazy API</h2>
 *
 * With the lazy API Polars doesn't run each query line-by-line but instead processes the full query
 * end-to-end. To get the most out of Polars it is important that you use the lazy API because:
 *
 * <ul>
 *   <li>the lazy API allows Polars to apply automatic query optimization with the query optimizer.
 *   <li>the lazy API allows you to work with larger than memory datasets using streaming.
 *   <li>the lazy API can catch schema errors before processing the data.
 * </ul>
 *
 * <p>More info can be found <a
 * href="https://pola-rs.github.io/polars-book/user-guide/lazy-api/intro.html">here</a>
 *
 * <p>
 *
 * <h2>Eager API</h2>
 *
 * With eager API the queries are executed line-by-line in contrast to the lazy API.
 */
public class LazyAndEagerAPI {

  public static void main(String[] args) {
    /* Lazily read data from file based datasets */
    String path = CommonUtils.getResource("/files/web-ds/data.csv");
    LazyFrame ldf = Polars.scan().option("scan_csv_n_rows", "2").csv(path);

    /* Materialize LazyFrame to DataFrame */
    DataFrame df = ldf.collect();

    System.out.println("Showing CSV file as a DataFrame to stdout");
    df.show();

    System.out.printf("Total rows: %s%n%n", df.count());
    System.out.printf("Total columns: %s%n%n", df.schema().getFields().length);

    /* Lazily read only first 3 rows */
    df = Polars.scan().option("scan_csv_n_rows", "3").csv(path).collect();
    System.out.printf("Total rows: %s%n%n", df.count());

    System.out.println("Rows:");
    Iterator<Row> rows = df.rows();

    while (rows.hasNext()) {
      System.out.println(rows.next());
    }
    System.out.println("\n");

    /* Convert DataFrame back to LazyFrame */
    LazyFrame backToLdf = df.toLazy();
    System.out.printf("Show schema: %s%n%n", backToLdf.schema());

    /* Eagerly read data from file based datasets */
    df = Polars.scan().csv(path).collect();

    System.out.println("Showing CSV file as a DataFrame to stdout");
    df.show();
  }
}
