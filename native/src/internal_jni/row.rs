use anyhow::Context;
use jni::JNIEnv;
use jni::objects::*;
use jni::sys::*;
use jni_fn::jni_fn;
use num_traits::ToPrimitive;
use polars::prelude::*;
use rust_decimal::Decimal;

use crate::internal_jni::utils::{find_java_class, from_ptr, get_n_rows, string_to_j_string};
use crate::utils::error::ResultExt;

#[jni_fn("com.github.chitralverma.polars.internal.jni.row$")]
pub fn createIterator(_: JNIEnv, _: JClass, df_ptr: *mut DataFrame, nRows: jlong) -> jlong {
    let df = &mut from_ptr(df_ptr);

    let n_rows = get_n_rows(nRows);
    let ri = RowIterator::new(df, n_rows);
    Box::into_raw(Box::new(ri.clone())) as jlong
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.row$")]
pub fn advanceIterator(mut env: JNIEnv, _: JClass, ri_ptr: *mut RowIterator) -> jobjectArray {
    let ri = &mut from_ptr(ri_ptr);
    let adv = ri.advance();

    if let Some(next_avs) = adv {
        let j_array = env
            .new_object_array(
                next_avs.len() as jsize,
                "java/lang/Object",
                JObject::null(),
            )
            .context("Failed to initialize array for row values")
            .unwrap_or_throw(&mut env);

        for (i, any_value) in next_avs.into_iter().enumerate() {
            let wrapped = AnyValueWrapper(any_value.clone());
            let java_object = wrapped.into_java(&mut env);
            env.set_object_array_element(&j_array, i as jsize, java_object)
                .context(format!("Failed to set value `{any_value}` in row"))
                .unwrap_or_throw(&mut env);
        }

        j_array.as_raw()
    } else {
        JObjectArray::from(JObject::null()).as_raw()
    }
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.row$")]
pub fn schemaString(mut env: JNIEnv, _: JClass, ri_ptr: *mut RowIterator) -> jstring {
    let ri = &from_ptr(ri_ptr);

    serde_json::to_string(&ri.schema.to_arrow(CompatLevel::oldest()))
        .map(|schema_string| string_to_j_string(&mut env, schema_string, None::<&str>))
        .context("Failed to serialize schema")
        .unwrap_or_throw(&mut env)
}

#[derive(Clone)]
pub struct RowIterator<'a> {
    vals: Vec<AnyValue<'a>>,
    width: usize,
    start: usize,
    pub end: usize,
    pub schema: SchemaRef,
}

impl<'a> RowIterator<'a> {
    pub fn new(data_frame: &'a mut DataFrame, end: Option<usize>) -> Self {
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
            schema: schema.clone(),
            end: std::cmp::min(end.unwrap_or(data_frame.height()), data_frame.height()),
        }
    }

    pub fn advance(&mut self) -> Option<Vec<AnyValue<'a>>> {
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
                        fld.metadata.as_deref(),
                    ),
                )
            }
        });

        let map = env
            .new_object("java/util/HashMap", "()V", &[])
            .context("Failed to initialize map for struct field")
            .unwrap_or_throw(env);

        let j_map = JMap::from_env(env, &map)
            .context("Failed to initialize map for struct field")
            .unwrap_or_throw(env);

        for (name, s) in iter {
            let series = s
                .context(format!(
                    "Failed to retrieve series for struct field `{name}`"
                ))
                .unwrap_or_throw(env);

            let key = unsafe {
                JObject::from_raw(string_to_j_string(
                    env,
                    &name,
                    Some(format!("Failed to parse value `{name}` as a series name")),
                ))
            };

            // Get first value only as series was sliced beforehand
            let value = AnyValueWrapper(series.first().value().as_borrowed()).into_java(env);

            j_map
                .put(env, &key, &value)
                .context("Failed to put entry in map for struct field")
                .unwrap_or_throw(env);
        }

        map
    }
}

impl<'a> IntoJava<'a> for &[u8] {
    fn into_java(self, env: &mut JNIEnv<'a>) -> JObject<'a> {
        let byte_array = env
            .new_byte_array(self.len() as jsize)
            .context("Failed to initialize byte array for binary value")
            .unwrap_or_throw(env);

        env.set_byte_array_region(&byte_array, 0, unsafe {
            // Safe because `u8` and `jbyte` are compatible in layout
            std::slice::from_raw_parts(self.as_ptr() as *const jbyte, self.len())
        })
        .context("Failed to set data in byte array for binary value")
        .unwrap_or_throw(env);

        JObject::from(byte_array)
    }
}

impl<'a> IntoJava<'a> for Series {
    fn into_java(self, env: &mut JNIEnv<'a>) -> JObject<'a> {
        let j_list_obj = env
            .new_object("java/util/ArrayList", "()V", &[])
            .context("Failed to initialize an array for series values")
            .unwrap_or_throw(env);

        let j_list = JList::from_env(env, &j_list_obj)
            .context("Failed to initialize an array for series values")
            .unwrap_or_throw(env);

        for any_value in self.iter() {
            let wrapped = AnyValueWrapper(any_value.clone());
            let element = wrapped.into_java(env);
            j_list
                .add(env, &element)
                .context(format!("Failed to set value `{any_value}` from series"))
                .unwrap_or_throw(env);
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
            AnyValue::Decimal(num, _precision, scale) => {
                Decimal::from_i128_with_scale(num, scale as u32)
                    .to_f64()
                    .map_or_else(|| JObject::null(), |v| box_double(env, v))
            },
            AnyValue::String(s) => unsafe {
                JObject::from_raw(string_to_j_string(
                    env,
                    s,
                    Some(format!("Failed to parse string value `{s}` in row")),
                ))
            },
            AnyValue::StringOwned(s) => unsafe {
                JObject::from_raw(string_to_j_string(
                    env,
                    &s,
                    Some(format!("Failed to parse string value `{s}` in row")),
                ))
            },
            AnyValue::Struct(row_idx, arr, _) => arr.clone().sliced(row_idx, 1).into_java(env),
            _ => JObject::null(),
        }
    }
}

fn call_java_static_method<'a>(
    env: &mut JNIEnv<'a>,
    class: JClass,
    method_name: &str,
    method_sig: &str,
    args: &[JValue],
) -> JObject<'a> {
    env.call_static_method(class, method_name, method_sig, args)
        .and_then(|x| x.l())
        .context(format!("Failed to call static method `{method_name}`"))
        .unwrap_or_throw(env)
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
    let j_instant = env
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
        .context(format!("Failed to parse value `{timestamp}` into Instant"))
        .unwrap_or_throw(env);

    // If a timezone is specified, convert it to ZonedDateTime
    // TODO: recheck this. one branch is returning Instant and the other is returning ZonedDateTime
    if let Some(zone) = time_zone {
        let zone_id: JObject = unsafe {
            JObject::from_raw(string_to_j_string(
                env,
                zone.as_str(),
                Some(format!("Failed to parse value `{zone}` into ZoneId")),
            ))
        };
        let cls = find_java_class(env, "java/time/ZonedDateTime");
        call_java_static_method(
            env,
            cls,
            "ofInstant",
            "(Ljava/time/Instant;Ljava/time/ZoneId;)Ljava/time/ZonedDateTime;",
            &[JValue::Object(&j_instant), JValue::Object(&zone_id)],
        )
    } else {
        j_instant
    }
}
