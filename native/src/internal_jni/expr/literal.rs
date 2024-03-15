#![allow(non_snake_case)]

use std::str::FromStr;

use jni::objects::{JObject, JString};
use jni::sys::{jboolean, jdouble, jfloat, jint, jlong};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::export::chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::*;

use crate::internal_jni::utils::{expr_to_ptr, get_string};

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn nullLit(mut env: JNIEnv, object: JObject) -> jlong {
    let expr = NULL.lit();
    expr_to_ptr(&mut env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromString(mut env: JNIEnv, object: JObject, value: JString) -> jlong {
    let this_path = get_string(&mut env, value, "Unable to get/ convert value to UTF8.");
    let expr = lit(this_path);
    expr_to_ptr(&mut env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromBool(mut env: JNIEnv, object: JObject, value: jboolean) -> jlong {
    let expr = lit(value);
    expr_to_ptr(&mut env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromInt(mut env: JNIEnv, object: JObject, value: jint) -> jlong {
    let expr = lit(value);
    expr_to_ptr(&mut env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromLong(mut env: JNIEnv, object: JObject, value: jlong) -> jlong {
    let expr = lit(value);
    expr_to_ptr(&mut env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromFloat(mut env: JNIEnv, object: JObject, value: jfloat) -> jlong {
    let expr = lit(value);
    expr_to_ptr(&mut env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromDouble(mut env: JNIEnv, object: JObject, value: jdouble) -> jlong {
    let expr = lit(value);
    expr_to_ptr(&mut env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromDate(mut env: JNIEnv, object: JObject, value: JString) -> jlong {
    let ts_string = get_string(&mut env, value, "Unable to get/ convert value to UTF8.");
    let date = NaiveDate::from_str(ts_string.as_str()).unwrap();
    let expr = lit(date);
    expr_to_ptr(&mut env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.literal_expr$")]
pub fn fromTimestamp(mut env: JNIEnv, object: JObject, value: JString) -> jlong {
    let ts_string = get_string(&mut env, value, "Unable to get/ convert value to UTF8.");
    let timestamp = NaiveDateTime::from_str(ts_string.as_str()).unwrap();
    let expr = lit(timestamp);
    expr_to_ptr(&mut env, object, expr)
}
