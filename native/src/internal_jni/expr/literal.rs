#![allow(non_snake_case)]

use jni::objects::{JObject, JString};
use jni::sys::{jboolean, jdouble, jfloat, jint, jlong};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::export::chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::*;
use std::str::FromStr;

use crate::internal_jni::utils::{expr_to_ptr, get_string};

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn nullLit(env: JNIEnv, object: JObject) -> jlong {
    let expr = NULL.lit();
    expr_to_ptr(env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromString(env: JNIEnv, object: JObject, value: JString) -> jlong {
    let this_path = get_string(env, value, "Unable to get/ convert value to UTF8.");
    let expr = lit(this_path);
    expr_to_ptr(env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromBool(env: JNIEnv, object: JObject, value: jboolean) -> jlong {
    let expr = lit(value);
    expr_to_ptr(env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromInt(env: JNIEnv, object: JObject, value: jint) -> jlong {
    let expr = lit(value);
    expr_to_ptr(env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromLong(env: JNIEnv, object: JObject, value: jlong) -> jlong {
    let expr = lit(value);
    expr_to_ptr(env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromFloat(env: JNIEnv, object: JObject, value: jfloat) -> jlong {
    let expr = lit(value);
    expr_to_ptr(env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromDouble(env: JNIEnv, object: JObject, value: jdouble) -> jlong {
    let expr = lit(value);
    expr_to_ptr(env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromDate(env: JNIEnv, object: JObject, value: JString) -> jlong {
    let ts_string = get_string(env, value, "Unable to get/ convert value to UTF8.");
    let date = NaiveDate::from_str(ts_string.as_str()).unwrap();
    let expr = lit(date);
    expr_to_ptr(env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromTimestamp(env: JNIEnv, object: JObject, value: JString) -> jlong {
    let ts_string = get_string(env, value, "Unable to get/ convert value to UTF8.");
    let timestamp = NaiveDateTime::from_str(ts_string.as_str()).unwrap();
    let expr = lit(timestamp);
    expr_to_ptr(env, object, expr)
}
