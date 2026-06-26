package examples.java.expressions;

import static com.github.chitralverma.polars.functions.*;

import com.github.chitralverma.polars.Polars;
import com.github.chitralverma.polars.api.DataFrame;
import com.github.chitralverma.polars.api.LazyFrame;
import examples.scala.utils.CommonUtils;
import java.util.Random;

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
            .withColumn("lower_than_four", col("id").lessThanEqualTo(4))
            .filter(col("lower_than_four"))
            .withColumn("long_value", lit(new Random().nextLong()))
            .withColumn("date", lit(java.time.LocalDate.now()))
            .withColumn("time", lit(java.time.LocalTime.now()))
            .withColumn("current_ts", lit(java.time.ZonedDateTime.now()))
            .sort(asc("name"), true, false)
            .setSorted("name", false, false)
            .topK(2, "id", true, true, false)
            .limit(2) // .head(2)
            .tail(2)
            .drop("long_value")
            .rename("lower_than_four", "less_than_four")
            .dropNulls();

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
