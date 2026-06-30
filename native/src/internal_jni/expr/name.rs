use jni::objects::{JObject, JString};
use jni::{Env, NativeMethod, native_method};

use crate::internal_jni::handle::{ExprHandle, Handle};
use crate::internal_jni::utils::j_string_to_string;
use crate::utils::error::ThrowRuntimeException;

const KEEP_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.expressions.name_expr$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe ExprHandle => long },
    extern fn keep(expr: ExprHandle) -> ExprHandle,
    name = "keep",
};

fn keep<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().name().keep()))
}

const PREFIX_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.expressions.name_expr$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe ExprHandle => long },
    extern fn prefix(expr: ExprHandle, value: java.lang.String) -> ExprHandle,
    name = "prefix",
};

fn prefix<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    value: JString<'local>,
) -> anyhow::Result<ExprHandle> {
    let s_prefix = j_string_to_string(env, &value, Some("Failed to parse prefix"))?;
    Ok(ExprHandle::alloc(expr.get().name().prefix(&s_prefix)))
}

const SUFFIX_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.expressions.name_expr$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe ExprHandle => long },
    extern fn suffix(expr: ExprHandle, value: java.lang.String) -> ExprHandle,
    name = "suffix",
};

fn suffix<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    value: JString<'local>,
) -> anyhow::Result<ExprHandle> {
    let s_suffix = j_string_to_string(env, &value, Some("Failed to parse suffix"))?;
    Ok(ExprHandle::alloc(expr.get().name().suffix(&s_suffix)))
}

const TO_UPPERCASE_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.expressions.name_expr$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe ExprHandle => long },
    extern fn to_uppercase(expr: ExprHandle) -> ExprHandle,
    name = "toUppercase",
};

fn to_uppercase<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().name().to_uppercase()))
}

const TO_LOWERCASE_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.expressions.name_expr$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe ExprHandle => long },
    extern fn to_lowercase(expr: ExprHandle) -> ExprHandle,
    name = "toLowercase",
};

fn to_lowercase<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().name().to_lowercase()))
}

/// All native methods exported by this module.
pub const METHODS: &[NativeMethod] = &[
    KEEP_METHOD,
    PREFIX_METHOD,
    SUFFIX_METHOD,
    TO_UPPERCASE_METHOD,
    TO_LOWERCASE_METHOD,
];
