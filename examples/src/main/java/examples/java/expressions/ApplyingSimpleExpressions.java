package examples.java.expressions;

import static org.polars.scala.polars.functions.*;

import examples.scala.utils.CommonUtils;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Random;
import org.polars.scala.polars.Polars;
import org.polars.scala.polars.api.DataFrame;
import org.polars.scala.polars.api.LazyFrame;

public class ApplyingSimpleExpressions {
  public static void main(String[] args) {

    /* Read a dataset as a DataFrame lazily or eagerly */
    String path = CommonUtils.getResource("/files/web-ds/data.json");
    LazyFrame input = Polars.ndJson().scan(path);

    /* Apply multiple operations on the LazyFrame or DataFrame */
    LazyFrame ldf =
        input
            .cache()
            .select("id", "name")
            .with_column("lower_than_four", col("id").lessThanEqualTo(4))
            .filter(col("lower_than_four"))
            .with_column("long_value", lit(new Random().nextLong()))
            .with_column("current_ts", lit(Timestamp.from(Instant.now())))
            .sort(asc("name"), true, false)
            .top_k(2, "id", true, true, false)
            .limit(2) // .head(2)
            .tail(2)
            .drop("current_ts", "long_value")
            .rename("lower_than_four", "less_than_four")
            .drop_nulls();

    ldf = Polars.concat(ldf, new LazyFrame[] {ldf, ldf});
    ldf = ldf.unique();

    System.out.println("Showing LazyFrame plan to stdout.");
    ldf.explain();

    DataFrame df = ldf.collect();

    System.out.println("Showing resultant DataFrame to stdout.");
    df.show();

    System.out.printf("Total rows: %s%n%n", df.count());
  }
}
