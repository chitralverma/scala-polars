use anyhow::Context;
use jni::JNIEnv;
use jni::objects::{JClass, JLongArray};
use jni::sys::jlong;
use jni_fn::jni_fn;
use polars_plan::dsl::functions::{
    all_horizontal, any_horizontal, max_horizontal, mean_horizontal, min_horizontal, sum_horizontal,
};
use polars_plan::prelude::Expr;

use crate::internal_jni::conversion::JavaArrayToVec;
use crate::internal_jni::utils::{from_ptr, to_ptr};
use crate::utils::error::ResultExt;

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.functions_expr$")]
pub fn any_horizontal(mut env: JNIEnv, _: JClass, inputs: JLongArray) -> jlong {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(&mut env, inputs)
        .into_iter()
        .map(|ptr| (from_ptr(ptr as *mut Expr)).to_owned())
        .collect();

    let expr = any_horizontal(exprs)
        .context("Failed to run any_horizontal")
        .unwrap_or_throw(&mut env);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.functions_expr$")]
pub fn all_horizontal(mut env: JNIEnv, _: JClass, inputs: JLongArray) -> jlong {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(&mut env, inputs)
        .into_iter()
        .map(|ptr| (from_ptr(ptr as *mut Expr)).to_owned())
        .collect();

    let expr = all_horizontal(exprs)
        .context("Failed to run all_horizontal")
        .unwrap_or_throw(&mut env);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.functions_expr$")]
pub fn max_horizontal(mut env: JNIEnv, _: JClass, inputs: JLongArray) -> jlong {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(&mut env, inputs)
        .into_iter()
        .map(|ptr| (from_ptr(ptr as *mut Expr)).to_owned())
        .collect();

    let expr = max_horizontal(exprs)
        .context("Failed to run max_horizontal")
        .unwrap_or_throw(&mut env);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.functions_expr$")]
pub fn min_horizontal(mut env: JNIEnv, _: JClass, inputs: JLongArray) -> jlong {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(&mut env, inputs)
        .into_iter()
        .map(|ptr| (from_ptr(ptr as *mut Expr)).to_owned())
        .collect();

    let expr = min_horizontal(exprs)
        .context("Failed to run min_horizontal")
        .unwrap_or_throw(&mut env);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.functions_expr$")]
pub fn sum_horizontal(mut env: JNIEnv, _: JClass, inputs: JLongArray, ignore_nulls: bool) -> jlong {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(&mut env, inputs)
        .into_iter()
        .map(|ptr| (from_ptr(ptr as *mut Expr)).to_owned())
        .collect();

    let expr = sum_horizontal(exprs, ignore_nulls)
        .context("Failed to run sum_horizontal")
        .unwrap_or_throw(&mut env);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.functions_expr$")]
pub fn mean_horizontal(
    mut env: JNIEnv,
    _: JClass,
    inputs: JLongArray,
    ignore_nulls: bool,
) -> jlong {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(&mut env, inputs)
        .into_iter()
        .map(|ptr| (from_ptr(ptr as *mut Expr)).to_owned())
        .collect();

    let expr = mean_horizontal(exprs, ignore_nulls)
        .context("Failed to run mean_horizontal")
        .unwrap_or_throw(&mut env);
    to_ptr(expr)
}
