package com.github.chitralverma.polars;

import static com.github.chitralverma.polars.functions.*;

import com.github.chitralverma.polars.api.DataFrame;
import com.github.chitralverma.polars.api.types.*;
import com.github.chitralverma.polars.testing.AbstractPolarsJavaTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Java mirror of {@code DataTypeSuite}. Replicates behaviours tested in upstream py-polars
 * `tests/unit/datatypes/test_datatypes.py`.
 */
public class DataTypeTest extends AbstractPolarsJavaTest {

  @Test
  public void ffiNameSerializesToOfficialPolarsSerdeJson() {
    Assert.assertEquals("String", DataTypes.String.ffiName());
    Assert.assertEquals("Int32", DataTypes.Int32.ffiName());
    Assert.assertEquals("Float64", DataTypes.Float64.ffiName());
    Assert.assertEquals("Boolean", DataTypes.Boolean.ffiName());

    Assert.assertEquals("{\"Time\":\"Microseconds\"}", DataTypes.time("Microseconds").ffiName());
    Assert.assertEquals(
        "{\"Timestamp\":[\"Microseconds\",\"UTC\"]}",
        DataTypes.datetime("Microseconds", "UTC").ffiName());
    Assert.assertEquals(
        "{\"Duration\":\"Microseconds\"}", DataTypes.duration("Microseconds").ffiName());

    Assert.assertEquals(
        "{\"List\":{\"dtype\":\"Int32\"}}", DataTypes.list(DataTypes.Int32).ffiName());
    Assert.assertEquals(
        "{\"Struct\":[{\"name\":\"a\",\"dtype\":\"Int32\"}]}",
        DataTypes.struct(new Field[] {new Field("a", DataTypes.Int32)}).ffiName());
  }

  @Test
  public void datatypeEqualityAndTimeUnits() {
    Assert.assertNotEquals(DataTypes.datetime("ms", "UTC"), DataTypes.datetime("ns", "UTC"));
    Assert.assertNotEquals(DataTypes.duration("ns"), DataTypes.duration("us"));

    Assert.assertEquals(DataTypes.datetime("us", "UTC"), DataTypes.datetime("us", "UTC"));
    Assert.assertEquals(DataTypes.duration("us"), DataTypes.duration("us"));
  }

  @Test
  public void castEndToEnd() {
    DataFrame df = intFrame("a", 1, 2, 3);

    // Cast Int32 -> Int16
    DataFrame result1 = df.withColumn("b", col("a").cast(DataTypes.Int16));
    assertColumns(result1, "a", "b");
    Assert.assertEquals(Int16Type$.MODULE$, result1.schema().getField("b").get().dataType());

    // Cast Int32 -> Float64
    DataFrame result2 = df.withColumn("b", col("a").cast(DataTypes.Float64));
    Assert.assertEquals(Float64Type$.MODULE$, result2.schema().getField("b").get().dataType());
    assertColumnValues(result2, "b", 1.0, 2.0, 3.0);
  }

  @Test
  public void isInUnaryOp() {
    DataFrame df = intFrame("a", 1, 2, 3, 4);
    DataFrame result = df.filter(col("a").isIn(new Object[] {2, 4}));

    assertRowCount(result, 2);
    assertColumnValues(result, "a", 2, 4);
  }

  @Test
  public void isBetweenUnaryOp() {
    DataFrame df = intFrame("a", 1, 2, 3, 4);
    DataFrame result = df.filter(col("a").isBetween(2, 3));

    assertRowCount(result, 2);
    assertColumnValues(result, "a", 2, 3);
  }

  @Test
  public void stringToUppercase() {
    DataFrame df = stringFrame("a", "apple", "banana");
    DataFrame result = df.withColumn("b", col("a").str().toUppercase());

    assertRowCount(result, 2);
    assertColumns(result, "a", "b");
    assertColumnValues(result, "b", "APPLE", "BANANA");
  }

  @Test
  public void likeWithWildcardMatching() {
    DataFrame df = stringFrame("a", "apple", "banana", "apricot");
    DataFrame result = df.filter(col("a").like("ap%"));

    assertRowCount(result, 2);
    assertColumnValues(result, "a", "apple", "apricot");
  }
}
