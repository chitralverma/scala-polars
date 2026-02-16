#![allow(non_snake_case)]

use std::iter::Iterator;

use anyhow::{Context, Error};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use jni::JNIEnv;
use jni::objects::*;
use jni::sys::jlong;
use jni_fn::jni_fn;
use polars::prelude::*;

use crate::internal_jni::conversion::JavaArrayToVec;
use crate::internal_jni::utils::{
    free_ptr, from_ptr, j_object_to_string, j_string_to_string, to_ptr,
};
use crate::utils::error::ResultExt;

macro_rules! impl_new_series {
    ($name:ident, $java_array:ty) => {
        #[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
        pub fn $name(mut env: JNIEnv, _: JClass, name: JString, values: $java_array) -> jlong {
            let data = JavaArrayToVec::to_vec(&mut env, values);

            let series_name = j_string_to_string(
                &mut env,
                &name,
                Some("Failed to parse the provided value as a series name"),
            );
            let series = Series::new(PlSmallStr::from_string(series_name), data);

            to_ptr(series)
        }
    };
}

impl_new_series!(new_long_series, JLongArray);
impl_new_series!(new_int_series, JIntArray);
impl_new_series!(new_float_series, JFloatArray);
impl_new_series!(new_double_series, JDoubleArray);
impl_new_series!(new_boolean_series, JBooleanArray);

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub fn new_str_series(mut env: JNIEnv, _: JClass, name: JString, values: JObjectArray) -> jlong {
    let data: Vec<String> = JavaArrayToVec::to_vec(&mut env, values)
        .into_iter()
        .map(|o| {
            j_object_to_string(
                &mut env,
                o,
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
pub fn new_date_series(mut env: JNIEnv, _: JClass, name: JString, values: JObjectArray) -> jlong {
    let data: Vec<NaiveDate> = JavaArrayToVec::to_vec(&mut env, values)
        .into_iter()
        .map(|o| {
            j_object_to_string(
                &mut env,
                o,
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
pub fn new_time_series(mut env: JNIEnv, _: JClass, name: JString, values: JObjectArray) -> jlong {
    let data: Vec<NaiveTime> = JavaArrayToVec::to_vec(&mut env, values)
        .into_iter()
        .map(|o| {
            j_object_to_string(
                &mut env,
                o,
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
pub fn new_datetime_series(
    mut env: JNIEnv,
    _: JClass,
    name: JString,
    values: JObjectArray,
) -> jlong {
    let data: Vec<NaiveDateTime> = JavaArrayToVec::to_vec(&mut env, values)
        .into_iter()
        .map(|o| {
            j_object_to_string(
                &mut env,
                o,
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
pub fn new_list_series(mut env: JNIEnv, _: JClass, name: JString, values: JLongArray) -> jlong {
    let data: Vec<Series> = JavaArrayToVec::to_vec(&mut env, values)
        .into_iter()
        .map(|ptr| from_ptr(ptr as *mut Series))
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
pub fn new_struct_series(mut env: JNIEnv, _: JClass, name: JString, values: JLongArray) -> jlong {
    let data: Vec<Series> = JavaArrayToVec::to_vec(&mut env, values)
        .into_iter()
        .map(|ptr| from_ptr(ptr as *mut Series))
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
pub fn show(_: JNIEnv, _: JClass, series_ptr: *mut Series) {
    let series = &from_ptr(series_ptr);
    println!("{series:?}")
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.series$")]
pub fn free(_: JNIEnv, _: JClass, ptr: jlong) {
    free_ptr::<Series>(ptr);
}
