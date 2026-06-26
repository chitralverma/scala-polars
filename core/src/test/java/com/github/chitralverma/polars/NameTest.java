package com.github.chitralverma.polars;

import static com.github.chitralverma.polars.functions.*;

import com.github.chitralverma.polars.api.DataFrame;
import com.github.chitralverma.polars.testing.AbstractPolarsJavaTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Java mirror of {@code NameSuite}. Replicates name modification behaviors and asserts Column
 * aliasing and name namespaces in Java.
 */
public class NameTest extends AbstractPolarsJavaTest {

  @Test
  public void expressionAliasAndJVMStyleAs() {
    DataFrame df = intFrame("ColX", 1, 2, 3);

    // Test alias
    DataFrame dfAlias = df.select(col("ColX").alias("AliasX"));
    Assert.assertTrue(dfAlias.schema().getField("AliasX").isDefined());
    Assert.assertFalse(dfAlias.schema().getField("ColX").isDefined());
    assertColumnValues(dfAlias, "AliasX", 1, 2, 3);

    // Test alias as
    DataFrame dfAs = df.select(col("ColX").as("AsX"));
    Assert.assertTrue(dfAs.schema().getField("AsX").isDefined());
    Assert.assertFalse(dfAs.schema().getField("ColX").isDefined());
    assertColumnValues(dfAs, "AsX", 1, 2, 3);

    // Test aliasing non-Column Expressions (e.g. literals)
    DataFrame dfLit = df.select(col("ColX"), lit(42).alias("LitX"));
    Assert.assertTrue(dfLit.schema().getField("LitX").isDefined());
    assertColumnValues(dfLit, "LitX", 42, 42, 42);
  }

  @Test
  public void nameChangeCase() {
    DataFrame df = intFrame("ColX", 1, 2, 3);

    // Test toUppercase
    DataFrame dfUpper = df.select(col("ColX").name().toUppercase());
    Assert.assertTrue(dfUpper.schema().getField("COLX").isDefined());
    Assert.assertFalse(dfUpper.schema().getField("ColX").isDefined());
    assertColumnValues(dfUpper, "COLX", 1, 2, 3);

    // Test toLowercase
    DataFrame dfLower = df.select(col("ColX").name().toLowercase());
    Assert.assertTrue(dfLower.schema().getField("colx").isDefined());
    Assert.assertFalse(dfLower.schema().getField("ColX").isDefined());
    assertColumnValues(dfLower, "colx", 1, 2, 3);
  }

  @Test
  public void namePrefixSuffix() {
    DataFrame df = intFrame("ColX", 1, 2, 3);

    // Test prefix
    DataFrame dfPrefix = df.select(col("ColX").name().prefix("pre_"));
    Assert.assertTrue(dfPrefix.schema().getField("pre_ColX").isDefined());
    Assert.assertFalse(dfPrefix.schema().getField("ColX").isDefined());
    assertColumnValues(dfPrefix, "pre_ColX", 1, 2, 3);

    // Test suffix
    DataFrame dfSuffix = df.select(col("ColX").name().suffix("_post"));
    Assert.assertTrue(dfSuffix.schema().getField("ColX_post").isDefined());
    Assert.assertFalse(dfSuffix.schema().getField("ColX").isDefined());
    assertColumnValues(dfSuffix, "ColX_post", 1, 2, 3);
  }

  @Test
  public void nameKeep() {
    DataFrame df = intFrame("ColX", 1, 2, 3);

    // Test keep
    DataFrame dfKeep = df.select(col("ColX").name().keep());
    Assert.assertTrue(dfKeep.schema().getField("ColX").isDefined());
    assertColumnValues(dfKeep, "ColX", 1, 2, 3);
  }
}
