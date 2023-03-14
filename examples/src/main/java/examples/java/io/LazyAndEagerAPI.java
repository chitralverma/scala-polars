package examples.java.io;

import examples.scala.utils.CommonUtils;
import org.polars.scala.polars.Polars;
import org.polars.scala.polars.api.DataFrame;
import org.polars.scala.polars.api.LazyFrame;

/**
 * Polars provides 2 API for reading datasets lazily ({@code scan}) or eagerly ({@code read}).
 * <p>
 * These APIs serve different purposes and result in either a {@link LazyFrame} or a {@link DataFrame}. A
 * LazyFrame can be materialized to a DataFrame and vice-versa if required.
 * </p>
 *
 * <p>
 * <h2>Lazy API</h2>
 * With the lazy API Polars doesn't run each query line-by-line but instead processes the full query end-to-end. To get the most out of
 * Polars it is important that you use the lazy API because:
 * <ul>
 *     <li>the lazy API allows Polars to apply automatic query optimization with the query optimizer.</li>
 *     <li>the lazy API allows you to work with larger than memory datasets using streaming.</li>
 *     <li>the lazy API can catch schema errors before processing the data.</li>
 * </ul>
 * <p>
 * More info can be found <a href="https://pola-rs.github.io/polars-book/user-guide/lazy-api/intro.html">here</a>
 * </p>
 *
 * <p>
 * <h2>Eager API</h2>
 * With eager API the queries are executed line-by-line in contrast to the lazy API.
 * </p>
 */

public class LazyAndEagerAPI {

    public static void main(String[] args) {
        /* Lazily read data from file based datasets */
        String path = CommonUtils.getResource("/files/web-ds/data.csv");
        LazyFrame ldf = Polars.csv().nRows(2).scan(path);

        /* Materialize LazyFrame to DataFrame */
        DataFrame df = ldf.collect();

        System.out.println("Showing CSV files as a DataFrame to stdout");
        df.show();

        System.out.printf("Total rows: %s%n%n", df.count());
        System.out.printf("Total columns: %s%n%n", df.schema().getFields().length);

        /* Lazily read only first 3 rows */
        df = Polars.csv().nRows(3).scan(path).collect();
        System.out.printf("Total rows: %s%n%n", df.count());

        /* Convert DataFrame back to LazyFrame */
        LazyFrame backToLdf = df.toLazy();
        System.out.printf("Show schema: %s%n%n", backToLdf.schema());

        /* Eagerly read data from file based datasets */
        df = Polars.csv().read(path);

        System.out.println("Showing CSV files as a DataFrame to stdout");
        df.show();
    }
}
