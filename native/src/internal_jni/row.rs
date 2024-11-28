use std::clone::Clone;

use jni::objects::{JClass, JList, JMap, JObject, JObjectArray, JValue};
use jni::sys::{jbyte, jlong, jobjectArray, jsize, jstring};
use jni::JNIEnv;
use jni_fn::jni_fn;
use num_traits::ToPrimitive;
use polars::prelude::*;
use rust_decimal::Decimal;

use crate::internal_jni::utils::get_n_rows;
use crate::j_data_frame::JDataFrame;

#[jni_fn("org.polars.scala.polars.internal.jni.row$")]
pub unsafe fn createIterator(
    _env: JNIEnv,
    _object: JObject,
    jdf_ptr: *mut JDataFrame,
    nRows: jlong,
) -> jlong {
    let n_rows = get_n_rows(nRows);
    let j_df = unsafe { &mut *jdf_ptr };
    let mut df = j_df.df.clone();

    let ri = RowIterator::new(&mut df, n_rows);
    Box::into_raw(Box::new(ri.clone())) as jlong
}

#[jni_fn("org.polars.scala.polars.internal.jni.row$")]
pub unsafe fn advanceIterator(
    mut env: JNIEnv,
    _: JClass,
    ri_ptr: *mut RowIterator,
) -> jobjectArray {
    let ri = unsafe { &mut *ri_ptr };
    let adv = ri.advance();

    if let Some(next_avs) = adv {
        let n_values = next_avs.len() as jsize;
        let jarray = env
            .new_object_array(n_values, "java/lang/Object", JObject::null())
            .expect("Unable to create array of row values");

        for (i, any_value) in next_avs.into_iter().enumerate() {
            let wrapped = AnyValueWrapper(any_value);
            let java_object = wrapped.into_java(&mut env);
            env.set_object_array_element(&jarray, i as jsize, java_object)
                .unwrap();
        }

        jarray.as_raw()
    } else {
        JObjectArray::from(JObject::null()).as_raw()
    }
}

#[jni_fn("org.polars.scala.polars.internal.jni.row$")]
pub unsafe fn schemaString(env: JNIEnv, _: JClass, ri_ptr: *mut RowIterator) -> jstring {
    let ri = unsafe { &*ri_ptr };
    let schema_string = serde_json::to_string(&ri.schema.to_arrow(CompatLevel::oldest())).unwrap();

    env.new_string(schema_string)
        .expect("Unable to get/ convert Schema to UTF8.")
        .into_raw()
}

#[derive(Clone)]
pub struct RowIterator<'a> {
    vals: Vec<AnyValue<'a>>,
    width: usize,
    start: usize,
    pub end: usize,
    pub schema: Schema,
}

impl<'a> RowIterator<'a> {
    pub fn new(data_frame: &'a mut DataFrame, end: Option<usize>) -> Self {
        data_frame.as_single_chunk_par();

        let width = data_frame.width();
        let size = width * data_frame.height();
        let mut buf = vec![AnyValue::Null; size];
        let schema = data_frame.schema();

        for (col_i, s) in data_frame.materialized_column_iter().enumerate() {
            for (row_i, av) in s.iter().enumerate() {
                buf[row_i * width + col_i] = av
            }
        }

        Self {
            vals: buf,
            width,
            start: 0,
            schema,
            end: std::cmp::min(end.unwrap_or(data_frame.height()), data_frame.height()),
        }
    }

    pub fn advance(&mut self) -> Option<Vec<AnyValue>> {
        if self.start < self.end {
            let start_index = self.start * self.width;
            let end_index = (self.start + 1) * self.width;
            let values = Vec::from(&self.vals[start_index..end_index]);

            self.start += 1;

            Some(values)
        } else {
            None
        }
    }
}

struct AnyValueWrapper<'a>(pub AnyValue<'a>);

pub trait IntoJava<'a> {
    /// Converts the implementing type into a JObject in Java
    fn into_java(self, env: &mut JNIEnv<'a>) -> JObject<'a>;
}

impl<'a> IntoJava<'a> for &StructArray {
    fn into_java(self, env: &mut JNIEnv<'a>) -> JObject<'a> {
        let (fld, _, arrs, _nulls) = self.clone().into_data();

        let iter = fld.iter().zip(arrs).map(|(fld, arr)| {
            // SAFETY:
            // reported data type is correct
            unsafe {
                (
                    fld.name.clone(),
                    Series::_try_from_arrow_unchecked_with_md(
                        fld.name.clone(),
                        vec![arr],
                        fld.dtype(),
                        Some(&fld.metadata),
                    ),
                )
            }
        });

        let map = env
            .new_object("java/util/HashMap", "()V", &[])
            .expect("Failed to create Java row map");
        let j_map = JMap::from_env(env, &map).unwrap();

        for (name, s) in iter {
            let series = s.expect("Unable to get series from struct");
            let java_object = AnyValueWrapper(series.first().value().as_borrowed()).into_java(env);

            let key = env.new_string(name).unwrap();
            j_map.put(env, &key, &java_object).unwrap();
        }

        map
    }
}

impl<'a> IntoJava<'a> for &[u8] {
    fn into_java(self, env: &mut JNIEnv<'a>) -> JObject<'a> {
        let byte_array = env
            .new_byte_array(self.len() as jsize)
            .expect("Failed to create byte array");
        env.set_byte_array_region(&byte_array, 0, unsafe {
            // Safe because `u8` and `jbyte` are compatible in layout
            std::slice::from_raw_parts(self.as_ptr() as *const jbyte, self.len())
        })
        .expect("Failed to set element in byte array");

        JObject::from(byte_array)
    }
}

impl<'a> IntoJava<'a> for Series {
    fn into_java(self, env: &mut JNIEnv<'a>) -> JObject<'a> {
        let j_list_obj = env
            .new_object("java/util/ArrayList", "()V", &[])
            .expect("Failed to create nested Java ArrayList");
        let j_list = JList::from_env(env, &j_list_obj).unwrap();

        for any_value in self.iter() {
            let wrapped = AnyValueWrapper(any_value);
            let element = wrapped.into_java(env);
            j_list
                .add(env, &element)
                .expect("Failed to set element in byte array");
        }

        j_list_obj
    }
}

impl<'a> IntoJava<'a> for AnyValueWrapper<'_> {
    fn into_java(self, env: &mut JNIEnv<'a>) -> JObject<'a> {
        match self.0 {
            AnyValue::Int8(v) => box_integer(env, v),
            AnyValue::Int16(v) => box_integer(env, v),
            AnyValue::Int32(v) => box_integer(env, v),
            AnyValue::UInt8(v) => box_integer(env, v),
            AnyValue::UInt16(v) => box_integer(env, v),
            AnyValue::UInt32(v) => box_integer(env, v as i32),
            AnyValue::Int64(v) => box_long(env, v),
            AnyValue::UInt64(v) => box_long(env, v as i64),
            AnyValue::Float32(v) => box_float(env, v),
            AnyValue::Float64(v) => box_double(env, v),
            AnyValue::Date(days) => box_date(env, days as i64),
            AnyValue::Time(v) => box_time(env, v),
            AnyValue::Datetime(nanos, tu, tz) => box_datetime(env, nanos, tu, tz),
            AnyValue::DatetimeOwned(nanos, tu, tz) => box_datetime(env, nanos, tu, tz.as_deref()),
            AnyValue::List(s) => s.into_java(env),
            AnyValue::Array(s, _) => s.into_java(env),
            AnyValue::Binary(slice) => slice.into_java(env),
            AnyValue::BinaryOwned(v) => v.as_slice().into_java(env),
            AnyValue::Boolean(v) => {
                box_primitive(env, v as u8, "java/lang/Boolean", "(Z)Ljava/lang/Boolean;")
            },
            AnyValue::Decimal(num, scale) => Decimal::from_i128_with_scale(num, scale as u32)
                .to_f64()
                .map_or_else(|| JObject::null(), |v| box_double(env, v)),
            AnyValue::String(v) => env
                .new_string(v)
                .expect("Failed to create Java string")
                .into(),
            AnyValue::StringOwned(v) => env
                .new_string(v)
                .expect("Failed to create Java string")
                .into(),
            AnyValue::Struct(row_idx, arr, _) => arr.clone().sliced(row_idx, 1).into_java(env),
            _ => JObject::null(),
        }
    }
}

fn find_java_class<'a>(env: &mut JNIEnv<'a>, class: &str) -> JClass<'a> {
    env.find_class(class)
        .expect(&format!("Failed to find Java class '{class}'"))
}

fn call_java_static_method<'a>(
    env: &mut JNIEnv<'a>,
    class: JClass,
    method_name: &str,
    method_sig: &str,
    args: &[JValue],
) -> JObject<'a> {
    env.call_static_method(class, method_name, method_sig, args)
        .expect(&format!("Failed to call static method '{method_name}'",))
        .l()
        .expect("Failed to cast boxed primitive to JObject")
}

/// Helper function to box primitive values into their corresponding Java wrapper classes
fn box_primitive<'a, T: Into<JValue<'a, 'a>>>(
    env: &mut JNIEnv<'a>,
    value: T,
    class: &str,
    method_sig: &str,
) -> JObject<'a> {
    let wrapper_class = find_java_class(env, class);
    call_java_static_method(env, wrapper_class, "valueOf", method_sig, &[value.into()])
}

fn box_integer<'a, T: Into<JValue<'a, 'a>>>(env: &mut JNIEnv<'a>, value: T) -> JObject<'a> {
    box_primitive(env, value, "java/lang/Integer", "(I)Ljava/lang/Integer;")
}

fn box_long<'a, T: Into<JValue<'a, 'a>>>(env: &mut JNIEnv<'a>, value: T) -> JObject<'a> {
    box_primitive(env, value, "java/lang/Long", "(J)Ljava/lang/Long;")
}

fn box_float<'a, T: Into<JValue<'a, 'a>>>(env: &mut JNIEnv<'a>, value: T) -> JObject<'a> {
    box_primitive(env, value, "java/lang/Float", "(F)Ljava/lang/Float;")
}

fn box_double<'a, T: Into<JValue<'a, 'a>>>(env: &mut JNIEnv<'a>, value: T) -> JObject<'a> {
    box_primitive(env, value, "java/lang/Double", "(D)Ljava/lang/Double;")
}

fn box_date<'a, T: Into<JValue<'a, 'a>>>(env: &mut JNIEnv<'a>, value: T) -> JObject<'a> {
    let wrapper_class = find_java_class(env, "java/time/LocalDate");
    call_java_static_method(
        env,
        wrapper_class,
        "ofEpochDay",
        "(J)Ljava/time/LocalDate;",
        &[value.into()],
    )
}

fn box_time<'a, T: Into<JValue<'a, 'a>>>(env: &mut JNIEnv<'a>, value: T) -> JObject<'a> {
    let wrapper_class = find_java_class(env, "java/time/LocalTime");
    call_java_static_method(
        env,
        wrapper_class,
        "ofNanoOfDay",
        "(J)Ljava/time/LocalTime;",
        &[value.into()],
    )
}

fn box_datetime<'a>(
    env: &mut JNIEnv<'a>,
    timestamp: i64,
    time_unit: TimeUnit,
    time_zone: Option<&TimeZone>,
) -> JObject<'a> {
    let nanos = match time_unit {
        TimeUnit::Nanoseconds => timestamp,
        TimeUnit::Microseconds => timestamp * 1_000,
        TimeUnit::Milliseconds => timestamp * 1_000_000,
    };
    let jzoned_date_time = env
        .call_static_method(
            "java/time/Instant",
            "ofEpochSecond",
            "(JJ)Ljava/time/Instant;",
            &[
                JValue::Long(nanos / 1_000_000_000),
                JValue::Long((nanos % 1_000_000_000) as jlong),
            ],
        )
        .ok()
        .and_then(|v| v.l().ok())
        .expect("Cant");

    // If a timezone is specified, convert it to ZonedDateTime
    if let Some(zone) = time_zone {
        let zone_id: JObject = env.new_string(zone).expect("Cant 2").into();
        let cls = find_java_class(env, "java/time/ZonedDateTime");
        call_java_static_method(
            env,
            cls,
            "ofInstant",
            "(Ljava/time/Instant;Ljava/time/ZoneId;)Ljava/time/ZonedDateTime;",
            &[JValue::Object(&jzoned_date_time), JValue::Object(&zone_id)],
        )
    } else {
        jzoned_date_time
    }
}
