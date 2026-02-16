use anyhow::Context;
use jni::JNIEnv;
use jni::objects::ReleaseMode::NoCopyBack;
use jni::objects::*;
use jni::sys::*;
use polars::prelude::*;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::internal_jni::utils::{find_java_class, string_to_j_string};
use crate::utils::error::ResultExt;

pub trait IntoJava<'a> {
    /// Converts the implementing type into a JObject in Java
    fn into_java(self, env: &mut JNIEnv<'a>) -> JObject<'a>;
}

pub struct AnyValueWrapper<'a>(pub AnyValue<'a>);

pub trait JavaArrayToVec {
    type Output;
    type InternalType;

    fn get_elements<'local, 'array, 'other_local, 'env>(
        env: &'env mut JNIEnv<'local>,
        array: &'array JPrimitiveArray<'other_local, <Self as JavaArrayToVec>::InternalType>,
    ) -> AutoElementsCritical<'local, 'other_local, 'array, 'env, Self::InternalType>
    where
        <Self as JavaArrayToVec>::InternalType: TypeArray,
    {
        unsafe {
            let mut cloned_env = env.unsafe_clone();
            env.get_array_elements_critical(array, NoCopyBack)
                .context("Failed to get elements of the array")
                .unwrap_or_throw(&mut cloned_env)
        }
    }

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output>;
}

impl JavaArrayToVec for JBooleanArray<'_> {
    type Output = bool;
    type InternalType = jboolean;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.iter().map(|&jb| jb == JNI_TRUE).collect()
    }
}

impl JavaArrayToVec for JIntArray<'_> {
    type Output = i32;
    type InternalType = jint;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.iter().copied().collect()
    }
}

impl JavaArrayToVec for JLongArray<'_> {
    type Output = i64;
    type InternalType = jlong;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.iter().copied().collect()
    }
}

impl JavaArrayToVec for JFloatArray<'_> {
    type Output = f32;
    type InternalType = jfloat;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.iter().copied().collect()
    }
}

impl JavaArrayToVec for JDoubleArray<'_> {
    type Output = f64;
    type InternalType = jdouble;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.iter().copied().collect()
    }
}

impl JavaArrayToVec for JObjectArray<'_> {
    type Output = jobject;
    type InternalType = jobject;
    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let len = env
            .get_array_length(&array)
            .context("Error getting length of the array")
            .unwrap_or_throw(env);
        let mut result = Vec::with_capacity(len as usize);

        for i in 0..len {
            let obj = env
                .get_object_array_element(&array, i)
                .context("Error getting element of the array")
                .unwrap_or_throw(env);
            result.push(obj.into_raw());
        }

        result
    }
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
            AnyValue::Datetime(nanos, tu, tz) => box_datetime(env, nanos, tu, tz.as_ref().map(|v| *v)),
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

pub fn call_java_static_method<'a>(
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
pub fn box_primitive<'a, T: Into<JValue<'a, 'a>>>(
    env: &mut JNIEnv<'a>,
    value: T,
    class: &str,
    method_sig: &str,
) -> JObject<'a> {
    let wrapper_class = find_java_class(env, class);
    call_java_static_method(env, wrapper_class, "valueOf", method_sig, &[value.into()])
}

pub fn box_integer<'a, T: Into<JValue<'a, 'a>>>(env: &mut JNIEnv<'a>, value: T) -> JObject<'a> {
    box_primitive(env, value, "java/lang/Integer", "(I)Ljava/lang/Integer;")
}

pub fn box_long<'a, T: Into<JValue<'a, 'a>>>(env: &mut JNIEnv<'a>, value: T) -> JObject<'a> {
    box_primitive(env, value, "java/lang/Long", "(J)Ljava/lang/Long;")
}

pub fn box_float<'a, T: Into<JValue<'a, 'a>>>(env: &mut JNIEnv<'a>, value: T) -> JObject<'a> {
    box_primitive(env, value, "java/lang/Float", "(F)Ljava/lang/Float;")
}

pub fn box_double<'a, T: Into<JValue<'a, 'a>>>(env: &mut JNIEnv<'a>, value: T) -> JObject<'a> {
    box_primitive(env, value, "java/lang/Double", "(D)Ljava/lang/Double;")
}

pub fn box_date<'a, T: Into<JValue<'a, 'a>>>(env: &mut JNIEnv<'a>, value: T) -> JObject<'a> {
    let wrapper_class = find_java_class(env, "java/time/LocalDate");
    call_java_static_method(
        env,
        wrapper_class,
        "ofEpochDay",
        "(J)Ljava/time/LocalDate;",
        &[value.into()],
    )
}

pub fn box_time<'a, T: Into<JValue<'a, 'a>>>(env: &mut JNIEnv<'a>, value: T) -> JObject<'a> {
    let wrapper_class = find_java_class(env, "java/time/LocalTime");
    call_java_static_method(
        env,
        wrapper_class,
        "ofNanoOfDay",
        "(J)Ljava/time/LocalTime;",
        &[value.into()],
    )
}

pub fn box_datetime<'a>(
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
        .and_then(|v| v.l())
        .context(format!("Failed to parse value `{timestamp}` into Instant"))
        .unwrap_or_throw(env);

    if let Some(zone) = time_zone {
        let zone_str_obj = unsafe {
            JObject::from_raw(string_to_j_string(
                env,
                zone.as_str(),
                Some(format!("Failed to parse value `{zone}` as a string")),
            ))
        };
        let zone_id = env
            .call_static_method(
                "java/time/ZoneId",
                "of",
                "(Ljava/lang/String;)Ljava/time/ZoneId;",
                &[JValue::Object(&zone_str_obj)],
            )
            .and_then(|v| v.l())
            .context(format!("Failed to parse value `{zone}` into ZoneId"))
            .unwrap_or_throw(env);

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
