#![allow(non_snake_case)]

use anyhow::Context;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jdouble, jfloat, jint, jlong};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::prelude::*;

use crate::internal_jni::utils::{j_string_to_string, to_ptr};
use crate::utils::error::ResultExt;

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.literal_expr$")]
pub fn nullLit(_: JNIEnv, _: JClass) -> jlong {
    let expr = NULL.lit();
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.literal_expr$")]
pub fn fromString(mut env: JNIEnv, _: JClass, value: JString) -> jlong {
    let string_value = j_string_to_string(
        &mut env,
        &value,
        Some("Failed to parse provided literal value as string"),
    );
    let expr = lit(string_value);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.literal_expr$")]
pub fn fromBool(_: JNIEnv, _: JClass, value: jboolean) -> jlong {
    let expr = lit(value);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.literal_expr$")]
pub fn fromInt(_: JNIEnv, _: JClass, value: jint) -> jlong {
    let expr = lit(value);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.literal_expr$")]
pub fn fromLong(_: JNIEnv, _: JClass, value: jlong) -> jlong {
    let expr = lit(value);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.literal_expr$")]
pub fn fromFloat(_: JNIEnv, _: JClass, value: jfloat) -> jlong {
    let expr = lit(value);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.literal_expr$")]
pub fn fromDouble(_: JNIEnv, _: JClass, value: jdouble) -> jlong {
    let expr = lit(value);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.literal_expr$")]
pub fn fromDate(mut env: JNIEnv, _: JClass, value: JString) -> jlong {
    let string_value = j_string_to_string(
        &mut env,
        &value,
        Some("Failed to parse provided literal value as string"),
    );

    let date = NaiveDate::parse_from_str(string_value.as_str(), "%Y-%m-%d")
        .context(format!(
            "Failed to parse value `{string_value}` as date with format `%Y-%m-%d`"
        ))
        .unwrap_or_throw(&mut env);

    let expr = lit(date);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.literal_expr$")]
pub fn fromTime(mut env: JNIEnv, _: JClass, value: JString) -> jlong {
    let string_value = j_string_to_string(
        &mut env,
        &value,
        Some("Failed to parse provided literal value as string"),
    );

    let time = NaiveTime::parse_from_str(string_value.as_str(), "%H:%M:%S%.f")
        .context(format!(
            "Failed to parse value `{string_value}` as time with format `%H:%M:%S%.f`"
        ))
        .unwrap_or_throw(&mut env);

    let total_seconds = time.num_seconds_from_midnight() as i64;
    let nanos = time.nanosecond() as i64;

    let expr = LiteralValue::Scalar(Scalar::new(
        DataType::Time,
        AnyValue::Time((total_seconds) * 1_000_000_000 + nanos),
    ))
    .lit();
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.literal_expr$")]
pub fn fromDateTime(mut env: JNIEnv, _: JClass, value: JString) -> jlong {
    let string_value = j_string_to_string(
        &mut env,
        &value,
        Some("Failed to parse provided literal value as string"),
    );

    let datetime = NaiveDateTime::parse_from_str(string_value.as_str(), "%FT%T%.f")
        .context(format!(
            "Failed to parse value `{string_value}` as datetime with format `%FT%T%.f`"
        ))
        .unwrap_or_throw(&mut env);

    let expr = lit(datetime);
    to_ptr(expr)
}
