package com.github.chitralverma.polars.testing;

import com.github.chitralverma.polars.api.DataFrame;
import com.github.chitralverma.polars.api.Row;
import com.github.chitralverma.polars.api.Series;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Assert;

/**
 * Shared base for Java test cases — the Java mirror of {@code PolarsTestBase}. A reusable test
 * base, not a per-test fixture: test classes extend it for frame-construction and assertion
 * helpers, and should name the upstream pytest they replicate (e.g. {@code
 * py-polars/tests/unit/operations/test_filter.py}) at the top of the file.
 */
public abstract class AbstractPolarsJavaTest {

  /**
   * Collect a {@link DataFrame} eagerly into row maps. {@code DataFrame.rows()} returns a {@code
   * scala.collection.Iterator}, whose {@code hasNext}/{@code next} are usable from Java directly.
   */
  protected List<Map<String, Object>> rowsOf(DataFrame df) {
    List<Map<String, Object>> out = new ArrayList<>();
    scala.collection.Iterator<Row> it = df.rows();
    while (it.hasNext()) {
      out.add(it.next().toJMap());
    }
    return out;
  }

  protected List<Object> columnOf(DataFrame df, String name) {
    List<Object> out = new ArrayList<>();
    for (Map<String, Object> row : rowsOf(df)) {
      out.add(row.get(name));
    }
    return out;
  }

  protected void assertRowCount(DataFrame df, long expected) {
    Assert.assertEquals(expected, df.count());
  }

  protected void assertColumns(DataFrame df, String... expected) {
    Assert.assertArrayEquals(expected, df.schema().getFieldNames());
  }

  protected void assertColumnValues(DataFrame df, String name, Object... expected) {
    List<Object> actual = columnOf(df, name);
    Assert.assertEquals("column '" + name + "' size mismatch", expected.length, actual.size());
    for (int i = 0; i < expected.length; i++) {
      Assert.assertEquals("column '" + name + "' at index " + i, expected[i], actual.get(i));
    }
  }

  protected DataFrame intFrame(String name, int... values) {
    return DataFrame.fromSeries(Series.ofInt(name, values));
  }

  protected DataFrame longFrame(String name, long... values) {
    return DataFrame.fromSeries(Series.ofLong(name, values));
  }

  protected DataFrame stringFrame(String name, String... values) {
    return DataFrame.fromSeries(Series.ofString(name, values));
  }

  protected DataFrame doubleFrame(String name, double... values) {
    return DataFrame.fromSeries(Series.ofDouble(name, values));
  }

  protected DataFrame booleanFrame(String name, boolean... values) {
    return DataFrame.fromSeries(Series.ofBoolean(name, values));
  }
}
