package com.github.chitralverma.polars;

import static com.github.chitralverma.polars.functions.*;

import com.github.chitralverma.polars.api.DataFrame;
import com.github.chitralverma.polars.api.types.DataTypes;
import com.github.chitralverma.polars.api.types.Int32Type$;
import com.github.chitralverma.polars.api.types.Int64Type$;
import com.github.chitralverma.polars.testing.AbstractPolarsJavaTest;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Java mirror of {@code MathSuite}. Tests arithmetic, rounding, logarithmic, and trigonometric
 * expression transforms, including null propagation and the error cases the operations reject.
 */
public class MathTest extends AbstractPolarsJavaTest {

  private static final double TOL = 1e-6;

  private List<Object> valuesOf(DataFrame df, String name) {
    return columnOf(df, name);
  }

  private void assertApprox(DataFrame df, String name, Object... expected) {
    List<Object> actual = columnOf(df, name);
    Assert.assertEquals("column '" + name + "' size mismatch", expected.length, actual.size());
    for (int i = 0; i < expected.length; i++) {
      Object e = expected[i];
      Object a = actual.get(i);
      if (e == null) {
        Assert.assertNull("index " + i, a);
      } else if (e instanceof Double) {
        double ed = (Double) e;
        double ad = ((Number) a).doubleValue();
        if (Double.isNaN(ed)) {
          Assert.assertTrue("index " + i + " expected NaN", Double.isNaN(ad));
        } else if (Double.isInfinite(ed)) {
          Assert.assertEquals("index " + i, ed, ad, 0.0);
        } else {
          Assert.assertEquals("index " + i, ed, ad, TOL);
        }
      } else {
        Assert.assertEquals("index " + i, e, a);
      }
    }
  }

  @Test
  public void negationPreservesNullsAndMatchesOperator() {
    DataFrame df = intFrame("a", -1, 0, 1, 5).withColumn("a", col("a").shift(1)); // [null,-1,0,1]
    assertApprox(df.select(col("a").neg().alias("r")), "r", null, 1, 0, -1);
  }

  @Test
  public void negationRejectsUnsignedAndNonNumeric() {
    DataFrame dfU = intFrame("a", 1, 2, 3);
    try {
      dfU.select(col("a").cast(DataTypes.UInt8).neg()).count();
      Assert.fail("Expected RuntimeException for unsigned negation");
    } catch (RuntimeException e) {
      // Expected.
    }

    DataFrame dfS = stringFrame("a", "p", "q", "r");
    try {
      dfS.select(col("a").neg()).count();
      Assert.fail("Expected RuntimeException for non-numeric negation");
    } catch (RuntimeException e) {
      // Expected.
    }
  }

  @Test
  public void powerRaisesToScalarExponent() {
    DataFrame df = doubleFrame("x", 2.0, 3.0, 4.0);
    assertApprox(df.select(col("x").pow(2.0).alias("r")), "r", 4.0, 9.0, 16.0);
    assertApprox(df.select(col("x").pow(0.0).alias("r")), "r", 1.0, 1.0, 1.0);
  }

  @Test
  public void floorDivisionRoundsTowardNegativeInfinity() {
    DataFrame df = intFrame("a", 1, 2, 3).withColumn("d", lit(2));
    assertApprox(df.select(col("a").floorDiv(col("d")).alias("r")), "r", 0, 1, 1);
  }

  @Test
  public void absoluteValuePreservesNullsAndRejectsNonNumeric() {
    DataFrame df = intFrame("a", -1, 0, 1, 5).withColumn("a", col("a").shift(1)); // [null,-1,0,1]
    assertApprox(df.select(col("a").abs().alias("r")), "r", null, 1, 0, 1);

    DataFrame dfF = doubleFrame("x", 1.0, -2.0, 3.0, -4.0);
    assertApprox(dfF.select(col("x").abs().alias("r")), "r", 1.0, 2.0, 3.0, 4.0);

    DataFrame dfS = stringFrame("a", "p", "q", "r");
    try {
      dfS.select(col("a").abs()).count();
      Assert.fail("Expected RuntimeException for non-numeric abs");
    } catch (RuntimeException e) {
      // Expected.
    }
  }

  @Test
  public void absoluteValueOfSignedMinimumWraps() {
    DataFrame df = intFrame("a", -128).withColumn("a", col("a").cast(DataTypes.Int8));
    List<Object> out = valuesOf(df.select(col("a").abs().alias("r")), "r");
    Assert.assertEquals(-128, ((Number) out.get(0)).intValue());
  }

  @Test
  public void floorAndCeil() {
    DataFrame dfFloor = doubleFrame("x", 1.4, 1.5, 2.5, -1.6);
    assertApprox(dfFloor.select(col("x").floor().alias("r")), "r", 1.0, 1.0, 2.0, -2.0);

    DataFrame dfCeil = doubleFrame("x", 1.8, 1.2, 3.0);
    assertApprox(dfCeil.select(col("x").ceil().alias("r")), "r", 2.0, 2.0, 3.0);
  }

  @Test
  public void roundUsesBankersRoundingAndIsIdentityOnIntegers() {
    DataFrame df = doubleFrame("x", 1.003, 2.003);
    assertApprox(df.select(col("x").round(2).alias("r")), "r", 1.0, 2.0);
    assertApprox(df.select(col("x").round().alias("r")), "r", 1.0, 2.0);

    DataFrame dfHalf = doubleFrame("x", 0.5, 1.5, 2.5, 3.5);
    assertApprox(dfHalf.select(col("x").round().alias("r")), "r", 0.0, 2.0, 2.0, 4.0);

    DataFrame dfInt = intFrame("a", 1, 2, 3);
    assertApprox(dfInt.select(col("a").round().alias("r")), "r", 1, 2, 3);
  }

  @Test
  public void roundSignificantFigures() {
    DataFrame dfF = doubleFrame("x", 1.234, 0.1234);
    assertApprox(dfF.select(col("x").roundSigFigs(2).alias("r")), "r", 1.2, 0.12);

    DataFrame dfI = intFrame("a", 123400, 1234);
    assertApprox(dfI.select(col("a").roundSigFigs(2).alias("r")), "r", 120000, 1200);

    DataFrame dfZero = doubleFrame("x", 0.0);
    assertApprox(dfZero.select(col("x").roundSigFigs(2).alias("r")), "r", 0.0);
  }

  @Test
  public void truncateDropsFractionAcrossSpecialValues() {
    DataFrame df = doubleFrame("x", 1.003, 2.003);
    assertApprox(df.select(col("x").truncate(2).alias("r")), "r", 1.0, 2.0);
    assertApprox(df.select(col("x").truncate().alias("r")), "r", 1.0, 2.0);

    DataFrame dfNeg = doubleFrame("x", -1.78, -2.56, -3.99);
    assertApprox(dfNeg.select(col("x").truncate(1).alias("r")), "r", -1.7, -2.5, -3.9);

    DataFrame dfInt = intFrame("a", 1, 2, 3);
    assertApprox(dfInt.select(col("a").truncate().alias("r")), "r", 1, 2, 3);

    DataFrame dfSpecial = doubleFrame("x", 1.5, Double.NaN, Double.POSITIVE_INFINITY);
    assertApprox(
        dfSpecial.select(col("x").truncate(1).alias("r")),
        "r",
        1.5,
        Double.NaN,
        Double.POSITIVE_INFINITY);
  }

  @Test
  public void signReturnsMinusOneZeroOnePreservingNullsAndNaN() {
    DataFrame dfInt =
        intFrame("a", -9, 0, 4, 7).withColumn("a", col("a").shift(1)); // [null,-9,0,4]
    assertApprox(dfInt.select(col("a").sign().alias("r")), "r", null, -1, 0, 1);

    DataFrame dfFloat = doubleFrame("x", -9.0, 0.0, 4.0, Double.NaN);
    assertApprox(dfFloat.select(col("x").sign().alias("r")), "r", -1.0, 0.0, 1.0, Double.NaN);

    DataFrame dfDate = intFrame("a", 1).withColumn("a", col("a").cast(DataTypes.Date));
    try {
      dfDate.select(col("a").sign()).count();
      Assert.fail("Expected RuntimeException for sign of date");
    } catch (RuntimeException e) {
      // Expected.
    }
  }

  @Test
  public void clipLimitsToRangeAndEachSingleBound() {
    DataFrame df = intFrame("a", 1, 2, 3, 4, 5).withColumn("mn", lit(0)).withColumn("mx", lit(2));
    assertApprox(df.select(col("a").clip(col("mn"), col("mx")).alias("r")), "r", 1, 2, 2, 2, 2);
    assertApprox(df.select(col("a").clipMin(col("mn")).alias("r")), "r", 1, 2, 3, 4, 5);
    assertApprox(df.select(col("a").clipMax(col("mx")).alias("r")), "r", 1, 2, 2, 2, 2);
  }

  @Test
  public void clipRejectsNonNumeric() {
    DataFrame dfS = stringFrame("a", "a", "b", "c");
    try {
      dfS.select(col("a").clip(lit("b"), lit("z"))).count();
      Assert.fail("Expected RuntimeException for non-numeric clip");
    } catch (RuntimeException e) {
      // Expected.
    }
  }

  @Test
  public void squareAndCubeRoots() {
    DataFrame dfSqrt = doubleFrame("x", 1.0, 2.0, 4.0);
    assertApprox(dfSqrt.select(col("x").sqrt().alias("r")), "r", 1.0, Math.sqrt(2.0), 2.0);

    DataFrame dfCbrt = doubleFrame("x", 1.0, 2.0, 8.0);
    assertApprox(dfCbrt.select(col("x").cbrt().alias("r")), "r", 1.0, Math.cbrt(2.0), 2.0);
  }

  @Test
  public void exponentialPreservesNullsAndRunsOnEmpty() {
    DataFrame df =
        doubleFrame("x", 0.1, 0.01, 99.0).withColumn("x", col("x").shift(1)); // [null,0.1,0.01]
    assertApprox(
        df.select(col("x").exp().alias("r")), "r", null, 1.1051709180756477, 1.010050167084168);

    DataFrame dfEmpty = doubleFrame("x").select(col("x").exp().alias("r"));
    Assert.assertTrue(valuesOf(dfEmpty, "r").isEmpty());
  }

  @Test
  public void logarithms() {
    DataFrame df = doubleFrame("x", 1.0, 3.0, 9.0, 27.0, 81.0);
    assertApprox(df.select(col("x").log(3.0).alias("r")), "r", 0.0, 1.0, 2.0, 3.0, 4.0);

    DataFrame dfE = doubleFrame("x", 1.0, Math.E);
    assertApprox(dfE.select(col("x").log().alias("r")), "r", 0.0, 1.0);

    DataFrame dfTen = doubleFrame("x", 1.0, 10.0, 1000.0);
    assertApprox(dfTen.select(col("x").log10().alias("r")), "r", 0.0, 1.0, 3.0);

    DataFrame dfLog1p = doubleFrame("x", 0.0, Math.E - 1.0);
    assertApprox(dfLog1p.select(col("x").log1p().alias("r")), "r", 0.0, 1.0);
  }

  @Test
  public void differenceWithLagAndNulls() {
    DataFrame df = intFrame("a", 10, 20, 30, 40);
    assertApprox(df.select(col("a").diff().alias("r")), "r", null, 10, 10, 10);
    assertApprox(df.select(col("a").diff(2).alias("r")), "r", null, null, 20, 20);
  }

  @Test
  public void percentageChangeWithLagAndNulls() {
    DataFrame dfPos = doubleFrame("x", 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0);
    assertApprox(
        dfPos.select(col("x").pctChange(2).alias("r")), "r", null, null, 3.0, 3.0, 3.0, 3.0, 3.0);

    DataFrame dfNull =
        doubleFrame("x", 10.0, 11.0, 12.0, 999.0, 12.0, 24.0)
            .withColumn("x", when(col("x").equalTo(999.0), lit(null)).otherwise(col("x")));
    assertApprox(
        dfNull.select(col("x").pctChange().alias("r")), "r", null, 0.1, 0.090909, null, null, 1.0);
  }

  @Test
  public void trigonometricInRadiansPreservesNullsAndNaN() {
    DataFrame df =
        doubleFrame("x", 0.0, Math.PI, Double.NaN, -1.0)
            .withColumn("x", col("x").shift(1)); // [null,0.0,Pi,NaN]

    assertApprox(
        df.select(col("x").sin().alias("r")), "r", null, 0.0, Math.sin(Math.PI), Double.NaN);
    assertApprox(df.select(col("x").cos().alias("r")), "r", null, 1.0, -1.0, Double.NaN);
    assertApprox(
        df.select(col("x").tan().alias("r")), "r", null, 0.0, Math.tan(Math.PI), Double.NaN);

    List<Object> cot = valuesOf(df.select(col("x").cot().alias("r")), "r");
    Assert.assertNull(cot.get(0));
    Assert.assertEquals(Double.POSITIVE_INFINITY, ((Number) cot.get(1)).doubleValue(), 0.0);
    Assert.assertTrue(Double.isNaN(((Number) cot.get(3)).doubleValue()));
  }

  @Test
  public void inverseAndHyperbolicTrigonometric() {
    DataFrame dfInv = doubleFrame("x", 0.0, 1.0);
    assertApprox(dfInv.select(col("x").arcsin().alias("r")), "r", 0.0, Math.PI / 2);
    assertApprox(dfInv.select(col("x").arccos().alias("r")), "r", Math.PI / 2, 0.0);
    assertApprox(dfInv.select(col("x").arctan().alias("r")), "r", 0.0, Math.PI / 4);

    DataFrame df = doubleFrame("x", 0.0, 1.0);
    assertApprox(df.select(col("x").sinh().alias("r")), "r", 0.0, Math.sinh(1.0));
    assertApprox(df.select(col("x").cosh().alias("r")), "r", 1.0, Math.cosh(1.0));
    assertApprox(df.select(col("x").tanh().alias("r")), "r", 0.0, Math.tanh(1.0));
    assertApprox(df.select(col("x").arcsinh().alias("r")), "r", 0.0, 0.881373587019543);
    assertApprox(df.select(col("x").arctanh().alias("r")), "r", 0.0, Double.POSITIVE_INFINITY);

    DataFrame dfCosh = doubleFrame("x", 1.0, 2.0);
    assertApprox(dfCosh.select(col("x").arccosh().alias("r")), "r", 0.0, 1.3169578969248166);
  }

  @Test
  public void trigonometricRejectsNonNumeric() {
    DataFrame dfS = stringFrame("a", "1", "2", "3");
    try {
      dfS.select(col("a").sin()).count();
      Assert.fail("Expected RuntimeException for non-numeric sin");
    } catch (RuntimeException e) {
      // Expected.
    }
  }

  @Test
  public void degreeAndRadianConversion() {
    DataFrame dfDeg = doubleFrame("x", 0.0, Math.PI);
    assertApprox(dfDeg.select(col("x").degrees().alias("r")), "r", 0.0, 180.0);

    DataFrame dfRad = doubleFrame("x", 0.0, 180.0);
    assertApprox(dfRad.select(col("x").radians().alias("r")), "r", 0.0, Math.PI);
  }

  @Test
  public void twoArgumentArctangentChoosesCorrectQuadrant() {
    DataFrame df = doubleFrame("y", 1.0, 1.0, -1.0).withColumn("x", lit(1.0));

    assertApprox(
        df.select(arctan2(col("y"), col("x")).alias("r")),
        "r",
        Math.PI / 4,
        Math.PI / 4,
        -Math.PI / 4);
    assertApprox(df.select(arctan2d(col("y"), col("x")).alias("r")), "r", 45.0, 45.0, -45.0);
    assertApprox(
        df.select(arctan2("y", "x").alias("r")), "r", Math.PI / 4, Math.PI / 4, -Math.PI / 4);
  }

  @Test
  public void physicalRepresentationOfIntegerIsIdentity() {
    DataFrame df = intFrame("a", 1, 2, 3);
    assertApprox(df.select(col("a").toPhysical().alias("r")), "r", 1, 2, 3);

    DataFrame dfDate = intFrame("a", 0, 1, 2).withColumn("a", col("a").cast(DataTypes.Date));
    DataFrame physical = dfDate.select(col("a").toPhysical().alias("r"));
    Assert.assertEquals(Int32Type$.MODULE$, physical.schema().getField("r").get().dataType());
  }

  @Test
  public void reinterpretUnsignedAsSignedKeepsBitPattern() {
    DataFrame df = longFrame("a", 1L, 1L, 2L);
    DataFrame out = df.select(col("a").cast(DataTypes.UInt64).reinterpret(true).alias("r"));
    assertApprox(out, "r", 1L, 1L, 2L);
    Assert.assertEquals(Int64Type$.MODULE$, out.schema().getField("r").get().dataType());
  }

  @Test
  public void inputValidationForRoundingParameters() {
    try {
      col("a").round(-1);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected.
    }
    try {
      col("a").roundSigFigs(0);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected.
    }
    try {
      col("a").truncate(-1);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected.
    }
  }
}
