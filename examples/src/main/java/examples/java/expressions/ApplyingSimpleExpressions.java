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
    LazyFrame ldf = Polars.ndJson().scan(path);

    /* Apply multiple operations on the LazyFrame or DataFrame */
    DataFrame df =
        ldf.select("id", "name")
            .withColumn("lower_than_four", col("id").lessThanEqualTo(4))
            .filter(col("lower_than_four"))
            .withColumn("long_value", lit(new Random().nextLong()))
            .withColumn("current_ts", lit(Timestamp.from(Instant.now())))
            .collect();

    System.out.println("Showing resultant DataFrame to stdout.");
    df.show();

    System.out.printf("Total rows: %s%n%n", df.count());
  }
}
