#![allow(non_snake_case)]

use std::iter::Iterator;

use anyhow::{Context, Error};
use jni::objects::*;
use jni::sys::jlong;
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::export::chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use polars::prelude::*;

use crate::internal_jni::utils::{j_string_to_string, to_ptr, JavaArrayToVec};
use crate::utils::error::ResultExt;

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub unsafe fn new_str_series(
    mut env: JNIEnv,
    _: JClass,
    name: JString,
    values: JObjectArray,
) -> jlong {
    let data: Vec<String> = JavaArrayToVec::to_vec(&mut env, values)
        .into_iter()
        .map(|o| JObject::from_raw(o))
        .map(|o| {
            j_string_to_string(
                &mut env,
                &JString::from(o),
                Some("Failed to parse the provided value as a series element"),
            )
        })
        .collect();

    let series_name = j_string_to_string(
        &mut env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    );
    let series = Series::new(PlSmallStr::from_string(series_name), data);

    to_ptr(series)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub fn new_long_series(mut env: JNIEnv, _: JClass, name: JString, values: JLongArray) -> jlong {
    let data = JavaArrayToVec::to_vec(&mut env, values);

    let series_name = j_string_to_string(
        &mut env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    );
    let series = Series::new(PlSmallStr::from_string(series_name), data);

    to_ptr(series)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub fn new_int_series(mut env: JNIEnv, _: JClass, name: JString, values: JIntArray) -> jlong {
    let data = JavaArrayToVec::to_vec(&mut env, values);

    let series_name = j_string_to_string(
        &mut env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    );
    let series = Series::new(PlSmallStr::from_string(series_name), data);

    to_ptr(series)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub fn new_float_series(mut env: JNIEnv, _: JClass, name: JString, values: JFloatArray) -> jlong {
    let data = JavaArrayToVec::to_vec(&mut env, values);

    let series_name = j_string_to_string(
        &mut env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    );
    let series = Series::new(PlSmallStr::from_string(series_name), data);

    to_ptr(series)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub fn new_double_series(mut env: JNIEnv, _: JClass, name: JString, values: JDoubleArray) -> jlong {
    let data = JavaArrayToVec::to_vec(&mut env, values);

    let series_name = j_string_to_string(
        &mut env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    );
    let series = Series::new(PlSmallStr::from_string(series_name), data);

    to_ptr(series)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub fn new_boolean_series(
    mut env: JNIEnv,
    _: JClass,
    name: JString,
    values: JBooleanArray,
) -> jlong {
    let data = JavaArrayToVec::to_vec(&mut env, values);

    let series_name = j_string_to_string(
        &mut env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    );
    let series = Series::new(PlSmallStr::from_string(series_name), data);

    to_ptr(series)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub unsafe fn new_date_series(
    mut env: JNIEnv,
    _: JClass,
    name: JString,
    values: JObjectArray,
) -> jlong {
    let data: Vec<NaiveDate> = JavaArrayToVec::to_vec(&mut env, values)
        .into_iter()
        .map(|o| JObject::from_raw(o))
        .map(|o| {
            j_string_to_string(
                &mut env,
                &JString::from(o),
                Some("Failed to parse the provided value as a series element"),
            )
        })
        .map(|s| {
            let lit = s.as_str();
            NaiveDate::parse_from_str(lit, "%Y-%m-%d").context(format!(
                "Failed to parse value `{lit}` as date with format `%Y-%m-%d`"
            ))
        })
        .collect::<Result<Vec<NaiveDate>, Error>>()
        .unwrap_or_throw(&mut env);

    let series_name = j_string_to_string(
        &mut env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    );
    let series = Series::new(PlSmallStr::from_string(series_name), data);

    to_ptr(series)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub unsafe fn new_time_series(
    mut env: JNIEnv,
    _: JClass,
    name: JString,
    values: JObjectArray,
) -> jlong {
    let data: Vec<NaiveTime> = JavaArrayToVec::to_vec(&mut env, values)
        .into_iter()
        .map(|o| JObject::from_raw(o))
        .map(|o| {
            j_string_to_string(
                &mut env,
                &JString::from(o),
                Some("Failed to parse the provided value as a series element"),
            )
        })
        .map(|s| {
            let lit = s.as_str();
            NaiveTime::parse_from_str(lit, "%H:%M:%S%.f").context(format!(
                "Failed to parse value `{lit}` as time with format `%H:%M:%S.f`"
            ))
        })
        .collect::<Result<Vec<NaiveTime>, Error>>()
        .unwrap_or_throw(&mut env);

    let series_name = j_string_to_string(
        &mut env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    );
    let series = Series::new(PlSmallStr::from_string(series_name), data);

    to_ptr(series)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub unsafe fn new_datetime_series(
    mut env: JNIEnv,
    _: JClass,
    name: JString,
    values: JObjectArray,
) -> jlong {
    let data: Vec<NaiveDateTime> = JavaArrayToVec::to_vec(&mut env, values)
        .into_iter()
        .map(|o| JObject::from_raw(o))
        .map(|o| {
            j_string_to_string(
                &mut env,
                &JString::from(o),
                Some("Failed to parse the provided value as a series element"),
            )
        })
        .map(|s| {
            let lit = s.as_str();
            NaiveDateTime::parse_from_str(lit, "%FT%T%.f").context(format!(
                "Failed to parse value `{lit}` as datetime with format `%FT%T%.f`"
            ))
        })
        .collect::<Result<Vec<NaiveDateTime>, Error>>()
        .unwrap_or_throw(&mut env);

    let series_name = j_string_to_string(
        &mut env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    );
    let series = Series::new(PlSmallStr::from_string(series_name), data);

    to_ptr(series)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub unsafe fn new_list_series(
    mut env: JNIEnv,
    _: JClass,
    name: JString,
    values: JLongArray,
) -> jlong {
    let data: Vec<Series> = JavaArrayToVec::to_vec(&mut env, values)
        .into_iter()
        .map(|ptr| (*(ptr as *mut Series)).to_owned())
        .collect();

    let series_name = j_string_to_string(
        &mut env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    );
    let series = Series::new(PlSmallStr::from_string(series_name), data);

    to_ptr(series)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub unsafe fn new_struct_series(
    mut env: JNIEnv,
    _: JClass,
    name: JString,
    values: JLongArray,
) -> jlong {
    let data: Vec<Series> = JavaArrayToVec::to_vec(&mut env, values)
        .into_iter()
        .map(|ptr| (*(ptr as *mut Series)).to_owned())
        .collect();

    let series_name = j_string_to_string(
        &mut env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    );
    let series = StructChunked::from_series(
        PlSmallStr::from_string(series_name),
        data.len(),
        data.iter(),
    )
    .context("Failed to create struct series from provided list of series")
    .unwrap_or_throw(&mut env)
    .into_series();

    to_ptr(series)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub unsafe fn show(_: JNIEnv, _: JClass, series_ptr: *mut Series) {
    let series = &*series_ptr;
    println!("{series:?}")
}
