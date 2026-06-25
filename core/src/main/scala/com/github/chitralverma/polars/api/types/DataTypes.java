package com.github.chitralverma.polars.api.types;

/**
 * Spark-style Java-friendly facade for all Polars DataTypes.
 * This class exposes public static final fields and static factory methods
 * so Java and Scala users can refer to types cleanly (e.g. {@code DataTypes.Int32}
 * instead of leaking Scala objects).
 */
public final class DataTypes {

  private DataTypes() {}

  // Basic singletons (Polars-faithful names)
  public static final DataType String = StringType$.MODULE$;
  public static final DataType Boolean = BooleanType$.MODULE$;
  public static final DataType Int8 = Int8Type$.MODULE$;
  public static final DataType Int16 = Int16Type$.MODULE$;
  public static final DataType Int32 = Int32Type$.MODULE$;
  public static final DataType Int64 = Int64Type$.MODULE$;
  public static final DataType UInt8 = UInt8Type$.MODULE$;
  public static final DataType UInt16 = UInt16Type$.MODULE$;
  public static final DataType UInt32 = UInt32Type$.MODULE$;
  public static final DataType UInt64 = UInt64Type$.MODULE$;
  public static final DataType Float32 = Float32Type$.MODULE$;
  public static final DataType Float64 = Float64Type$.MODULE$;
  public static final DataType Date = DateType$.MODULE$;
  public static final DataType Time = TimeType$.MODULE$;
  public static final DataType Null = NullType$.MODULE$;

  // Default DecimalType singleton (precision=None, scale=0)
  public static final DataType Decimal = new DecimalType(scala.Option.apply(null), 0);

  // Static Factory Methods for parametric types (S2 / Q2)

  /**
   * Create a Decimal DataType with the given scale.
   *
   * @param scale number of digits after the decimal point
   * @return Decimal DataType
   */
  public static DataType decimal(int scale) {
    return new DecimalType(scala.Option.apply(null), scale);
  }

  /**
   * Create a Decimal DataType with the given precision and scale.
   *
   * @param precision total number of digits
   * @param scale number of digits after the decimal point
   * @return Decimal DataType
   */
  public static DataType decimal(int precision, int scale) {
    return new DecimalType(scala.Option.apply(precision), scale);
  }

  /**
   * Create a Time DataType with the specified time unit.
   *
   * @param timeUnit time unit (e.g. "Microseconds", "Nanoseconds")
   * @return Time DataType
   */
  public static DataType time(String timeUnit) {
    return new TimeType(timeUnit);
  }

  /**
   * Create a Datetime DataType with the specified time unit and timezone.
   *
   * @param timeUnit time unit (e.g. "Microseconds")
   * @param timeZone timezone identifier (e.g. "UTC", "Asia/Tokyo") or null
   * @return Datetime DataType
   */
  public static DataType datetime(String timeUnit, String timeZone) {
    return new DateTimeType(timeUnit, timeZone);
  }

  /**
   * Create a Duration DataType with the specified time unit.
   *
   * @param timeUnit time unit (e.g. "Microseconds")
   * @return Duration DataType
   */
  public static DataType duration(String timeUnit) {
    return new Duration(timeUnit);
  }

  /**
   * Create a List DataType with the specified element type.
   *
   * @param elementType the DataType of the list's elements
   * @return List DataType
   */
  public static DataType list(DataType elementType) {
    return new ListType(elementType);
  }

  /**
   * Create a Struct DataType consisting of the specified fields.
   *
   * @param fields fields defining the struct's schema
   * @return Struct DataType
   */
  public static DataType struct(Field[] fields) {
    return new StructType(fields);
  }
}
