package examples.java.expressions;

import static org.polars.scala.polars.functions.*;

import examples.scala.utils.CommonUtils;
import java.util.Collections;
import java.util.Random;
import org.polars.scala.polars.Polars;
import org.polars.scala.polars.api.DataFrame;
import org.polars.scala.polars.api.LazyFrame;

public class ApplyingSimpleExpressions {

  public static void main(String[] args) {
    /* Read a dataset as a DataFrame lazily or eagerly */
    String path = CommonUtils.getResource("/files/web-ds/data.json");
    LazyFrame input = Polars.scan().jsonLines(path);

    /* Apply multiple operations on the LazyFrame or DataFrame */
    LazyFrame ldf =
        input
            .cache()
            .select("id", "name")
            .with_column("lower_than_four", col("id").lessThanEqualTo(4))
            .filter(col("lower_than_four"))
            .with_column("long_value", lit(new Random().nextLong()))
            .with_column("date", lit(java.time.LocalDate.now()))
            .with_column("time", lit(java.time.LocalTime.now()))
            .with_column("current_ts", lit(java.time.ZonedDateTime.now()))
            .sort(asc("name"), true, false)
            .set_sorted(Collections.singletonMap("name", false))
            .top_k(2, "id", true, true, false)
            .limit(2) // .head(2)
            .tail(2)
            .drop("long_value")
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
