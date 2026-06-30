use anyhow::Context;
use jni::objects::{JLongArray, JObject};
use jni::{Env, NativeMethod, native_method};
use polars_plan::dsl::functions::{
    all_horizontal, any_horizontal, max_horizontal, mean_horizontal, min_horizontal, sum_horizontal,
};
use polars_plan::prelude::Expr;

use crate::internal_jni::conversion::JavaArrayToVec;
use crate::internal_jni::handle::{ExprHandle, Handle};
use crate::utils::error::ThrowRuntimeException;

const ANY_HORIZONTAL_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.expressions.functions_expr$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe ExprHandle => long },
    extern fn any_horizontal_expr(inputs: [jlong]) -> ExprHandle,
    name = "anyHorizontal",
};

fn any_horizontal_expr<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    inputs: JLongArray<'local>,
) -> anyhow::Result<ExprHandle> {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(env, inputs)?
        .into_iter()
        .map(|ptr| ExprHandle::from(ptr).get())
        .collect();

    let expr = any_horizontal(exprs).context("Failed to run any_horizontal")?;
    Ok(ExprHandle::alloc(expr))
}

const ALL_HORIZONTAL_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.expressions.functions_expr$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe ExprHandle => long },
    extern fn all_horizontal_expr(inputs: [jlong]) -> ExprHandle,
    name = "allHorizontal",
};

fn all_horizontal_expr<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    inputs: JLongArray<'local>,
) -> anyhow::Result<ExprHandle> {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(env, inputs)?
        .into_iter()
        .map(|ptr| ExprHandle::from(ptr).get())
        .collect();

    let expr = all_horizontal(exprs).context("Failed to run all_horizontal")?;
    Ok(ExprHandle::alloc(expr))
}

const MAX_HORIZONTAL_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.expressions.functions_expr$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe ExprHandle => long },
    extern fn max_horizontal_expr(inputs: [jlong]) -> ExprHandle,
    name = "maxHorizontal",
};

fn max_horizontal_expr<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    inputs: JLongArray<'local>,
) -> anyhow::Result<ExprHandle> {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(env, inputs)?
        .into_iter()
        .map(|ptr| ExprHandle::from(ptr).get())
        .collect();

    let expr = max_horizontal(exprs).context("Failed to run max_horizontal")?;
    Ok(ExprHandle::alloc(expr))
}

const MIN_HORIZONTAL_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.expressions.functions_expr$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe ExprHandle => long },
    extern fn min_horizontal_expr(inputs: [jlong]) -> ExprHandle,
    name = "minHorizontal",
};

fn min_horizontal_expr<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    inputs: JLongArray<'local>,
) -> anyhow::Result<ExprHandle> {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(env, inputs)?
        .into_iter()
        .map(|ptr| ExprHandle::from(ptr).get())
        .collect();

    let expr = min_horizontal(exprs).context("Failed to run min_horizontal")?;
    Ok(ExprHandle::alloc(expr))
}

const SUM_HORIZONTAL_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.expressions.functions_expr$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe ExprHandle => long },
    extern fn sum_horizontal_expr(inputs: [jlong], ignore_nulls: bool) -> ExprHandle,
    name = "sumHorizontal",
};

fn sum_horizontal_expr<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    inputs: JLongArray<'local>,
    ignore_nulls: bool,
) -> anyhow::Result<ExprHandle> {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(env, inputs)?
        .into_iter()
        .map(|ptr| ExprHandle::from(ptr).get())
        .collect();

    let expr = sum_horizontal(exprs, ignore_nulls).context("Failed to run sum_horizontal")?;
    Ok(ExprHandle::alloc(expr))
}

const MEAN_HORIZONTAL_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.expressions.functions_expr$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe ExprHandle => long },
    extern fn mean_horizontal_expr(inputs: [jlong], ignore_nulls: bool) -> ExprHandle,
    name = "meanHorizontal",
};

fn mean_horizontal_expr<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    inputs: JLongArray<'local>,
    ignore_nulls: bool,
) -> anyhow::Result<ExprHandle> {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(env, inputs)?
        .into_iter()
        .map(|ptr| ExprHandle::from(ptr).get())
        .collect();

    let expr = mean_horizontal(exprs, ignore_nulls).context("Failed to run mean_horizontal")?;
    Ok(ExprHandle::alloc(expr))
}

/// All native methods exported by this module.
pub const METHODS: &[NativeMethod] = &[
    ANY_HORIZONTAL_METHOD,
    ALL_HORIZONTAL_METHOD,
    MAX_HORIZONTAL_METHOD,
    MIN_HORIZONTAL_METHOD,
    SUM_HORIZONTAL_METHOD,
    MEAN_HORIZONTAL_METHOD,
];
