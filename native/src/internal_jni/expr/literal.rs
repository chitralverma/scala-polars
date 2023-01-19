#![allow(non_snake_case)]

use jni::objects::{JObject, JString};
use jni::sys::{jboolean, jdouble, jfloat, jint, jlong};
use jni::JNIEnv;
use polars::export::chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::*;
use std::str::FromStr;

use crate::internal_jni::utils::{expr_to_ptr, get_string};

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_literal_1expr_00024_nullLit(
    env: JNIEnv,
    object: JObject,
) -> jlong {
    let expr = NULL.lit();
    expr_to_ptr(env, object, expr)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_literal_1expr_00024_fromString(
    env: JNIEnv,
    object: JObject,
    value: JString,
) -> jlong {
    let this_path = get_string(env, value, "Unable to get/ convert value to UTF8.");
    let expr = lit(this_path);
    expr_to_ptr(env, object, expr)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_literal_1expr_00024_fromBool(
    env: JNIEnv,
    object: JObject,
    value: jboolean,
) -> jlong {
    let expr = lit(value);
    expr_to_ptr(env, object, expr)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_literal_1expr_00024_fromInt(
    env: JNIEnv,
    object: JObject,
    value: jint,
) -> jlong {
    let expr = lit(value);
    expr_to_ptr(env, object, expr)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_literal_1expr_00024_fromLong(
    env: JNIEnv,
    object: JObject,
    value: jlong,
) -> jlong {
    let expr = lit(value);
    expr_to_ptr(env, object, expr)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_literal_1expr_00024_fromFloat(
    env: JNIEnv,
    object: JObject,
    value: jfloat,
) -> jlong {
    let expr = lit(value);
    expr_to_ptr(env, object, expr)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_literal_1expr_00024_fromDouble(
    env: JNIEnv,
    object: JObject,
    value: jdouble,
) -> jlong {
    let expr = lit(value);
    expr_to_ptr(env, object, expr)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_literal_1expr_00024_fromDate(
    env: JNIEnv,
    object: JObject,
    value: JString,
) -> jlong {
    let ts_string = get_string(env, value, "Unable to get/ convert value to UTF8.");
    let date = NaiveDate::from_str(ts_string.as_str()).unwrap();
    let expr = lit(date);
    expr_to_ptr(env, object, expr)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_literal_1expr_00024_fromTimestamp(
    env: JNIEnv,
    object: JObject,
    value: JString,
) -> jlong {
    let ts_string = get_string(env, value, "Unable to get/ convert value to UTF8.");
    let timestamp = NaiveDateTime::from_str(ts_string.as_str()).unwrap();
    let expr = lit(timestamp);
    expr_to_ptr(env, object, expr)
}
