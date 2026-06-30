use anyhow::Context;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use jni::objects::{JObject, JString};
use jni::sys::{jboolean, jdouble, jfloat, jint, jlong};
use jni::{Env, NativeMethod, native_method};
use polars::prelude::*;

use crate::internal_jni::handle::{ExprHandle, Handle};
use crate::internal_jni::utils::j_string_to_string;
use crate::utils::error::ThrowRuntimeException;

/// Injects the shared `literal_expr$` config into [`native_method!`].
macro_rules! lit_method {
    ($($tt:tt)*) => {
        native_method! {
            java_type = "com.github.chitralverma.polars.internal.jni.expressions.literal_expr$",
            error_policy = ThrowRuntimeException,
            type_map = { unsafe ExprHandle => long },
            $($tt)*
        }
    };
}

const NULL_LIT_METHOD: NativeMethod = lit_method!(
    extern fn null_lit() -> ExprHandle,
    name = "nullLit",
);

fn null_lit<'local>(_env: &mut Env<'local>, _this: JObject<'local>) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(NULL.lit()))
}

const FROM_STRING_METHOD: NativeMethod = lit_method!(
    extern fn from_string(value: java.lang.String) -> ExprHandle,
    name = "fromString",
);

fn from_string<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    value: JString<'local>,
) -> anyhow::Result<ExprHandle> {
    let string_value = j_string_to_string(
        env,
        &value,
        Some("Failed to parse provided literal value as string"),
    )?;
    Ok(ExprHandle::alloc(lit(string_value)))
}

const FROM_BOOL_METHOD: NativeMethod = lit_method!(
    extern fn from_bool(value: jboolean) -> ExprHandle,
    name = "fromBool",
);

fn from_bool<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    value: jboolean,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(lit(value)))
}

const FROM_INT_METHOD: NativeMethod = lit_method!(
    extern fn from_int(value: jint) -> ExprHandle,
    name = "fromInt",
);

fn from_int<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    value: jint,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(lit(value)))
}

const FROM_LONG_METHOD: NativeMethod = lit_method!(
    extern fn from_long(value: jlong) -> ExprHandle,
    name = "fromLong",
);

fn from_long<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    value: jlong,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(lit(value)))
}

const FROM_FLOAT_METHOD: NativeMethod = lit_method!(
    extern fn from_float(value: jfloat) -> ExprHandle,
    name = "fromFloat",
);

fn from_float<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    value: jfloat,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(lit(value)))
}

const FROM_DOUBLE_METHOD: NativeMethod = lit_method!(
    extern fn from_double(value: jdouble) -> ExprHandle,
    name = "fromDouble",
);

fn from_double<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    value: jdouble,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(lit(value)))
}

const FROM_DATE_METHOD: NativeMethod = lit_method!(
    extern fn from_date(value: java.lang.String) -> ExprHandle,
    name = "fromDate",
);

fn from_date<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    value: JString<'local>,
) -> anyhow::Result<ExprHandle> {
    let string_value = j_string_to_string(
        env,
        &value,
        Some("Failed to parse provided literal value as string"),
    )?;

    let date = NaiveDate::parse_from_str(string_value.as_str(), "%Y-%m-%d").context(format!(
        "Failed to parse value `{string_value}` as date with format `%Y-%m-%d`"
    ))?;

    Ok(ExprHandle::alloc(lit(date)))
}

const FROM_TIME_METHOD: NativeMethod = lit_method!(
    extern fn from_time(value: java.lang.String) -> ExprHandle,
    name = "fromTime",
);

fn from_time<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    value: JString<'local>,
) -> anyhow::Result<ExprHandle> {
    let string_value = j_string_to_string(
        env,
        &value,
        Some("Failed to parse provided literal value as string"),
    )?;

    let time = NaiveTime::parse_from_str(string_value.as_str(), "%H:%M:%S%.f").context(format!(
        "Failed to parse value `{string_value}` as time with format `%H:%M:%S%.f`"
    ))?;

    let total_seconds = time.num_seconds_from_midnight() as i64;
    let nanos = time.nanosecond() as i64;

    let expr = LiteralValue::Scalar(Scalar::new(
        DataType::Time,
        AnyValue::Time((total_seconds) * 1_000_000_000 + nanos),
    ))
    .lit();
    Ok(ExprHandle::alloc(expr))
}

const FROM_DATE_TIME_METHOD: NativeMethod = lit_method!(
    extern fn from_date_time(value: java.lang.String) -> ExprHandle,
    name = "fromDateTime",
);

fn from_date_time<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    value: JString<'local>,
) -> anyhow::Result<ExprHandle> {
    let string_value = j_string_to_string(
        env,
        &value,
        Some("Failed to parse provided literal value as string"),
    )?;

    let datetime = NaiveDateTime::parse_from_str(string_value.as_str(), "%FT%T%.f").context(
        format!("Failed to parse value `{string_value}` as datetime with format `%FT%T%.f`"),
    )?;

    Ok(ExprHandle::alloc(lit(datetime)))
}

pub const METHODS: &[NativeMethod] = &[
    NULL_LIT_METHOD,
    FROM_STRING_METHOD,
    FROM_BOOL_METHOD,
    FROM_INT_METHOD,
    FROM_LONG_METHOD,
    FROM_FLOAT_METHOD,
    FROM_DOUBLE_METHOD,
    FROM_DATE_METHOD,
    FROM_TIME_METHOD,
    FROM_DATE_TIME_METHOD,
];
