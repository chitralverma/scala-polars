package com.github.chitralverma.polars;

import static com.github.chitralverma.polars.functions.*;

import com.github.chitralverma.polars.api.DataFrame;
import com.github.chitralverma.polars.testing.AbstractPolarsJavaTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Harness self-test (Java) — mirror of {@code HarnessSmokeSuite}. Native lib loads under {@code
 * Test}, a frame round-trips an expression. Not a port of an upstream pytest.
 */
public class HarnessSmokeTest extends AbstractPolarsJavaTest {

  @Test
  public void nativeLibraryLoadsAndReportsAVersion() {
    String version = Polars.version();
    Assert.assertNotNull(version);
    Assert.assertFalse(version.trim().isEmpty());
  }

  @Test
  public void buildFrameApplyColPlusOneCollectAndReadBack() {
    DataFrame df = intFrame("a", 1, 2, 3);
    DataFrame result = df.with_column("b", col("a").plus(1));

    assertRowCount(result, 3);
    assertColumns(result, "a", "b");
    assertColumnValues(result, "b", 2, 3, 4);
  }

  @Test
  public void filterKeepsOnlyMatchingRows() {
    DataFrame df = intFrame("a", 1, 2, 3, 4);
    DataFrame result = df.filter(col("a").greaterThan(2));

    assertRowCount(result, 2);
    assertColumnValues(result, "a", 3, 4);
  }
}
