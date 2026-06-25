use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni_fn::jni_fn;
use polars::prelude::*;

use crate::internal_jni::utils::{from_ptr, j_string_to_string, to_ptr};

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.name_expr$")]
pub fn keep(_: JNIEnv, _: JClass, expr_ptr: *mut Expr) -> jlong {
    let l_expr = from_ptr(expr_ptr);
    let expr = l_expr.name().keep();
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.name_expr$")]
pub fn prefix(mut env: JNIEnv, _: JClass, expr_ptr: *mut Expr, prefix: JString) -> jlong {
    let l_expr = from_ptr(expr_ptr);
    let s_prefix = j_string_to_string(&mut env, &prefix, Some("Failed to parse prefix"));
    let expr = l_expr.name().prefix(&s_prefix);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.name_expr$")]
pub fn suffix(mut env: JNIEnv, _: JClass, expr_ptr: *mut Expr, suffix: JString) -> jlong {
    let l_expr = from_ptr(expr_ptr);
    let s_suffix = j_string_to_string(&mut env, &suffix, Some("Failed to parse suffix"));
    let expr = l_expr.name().suffix(&s_suffix);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.name_expr$")]
pub fn to_uppercase(_: JNIEnv, _: JClass, expr_ptr: *mut Expr) -> jlong {
    let l_expr = from_ptr(expr_ptr);
    let expr = l_expr.name().to_uppercase();
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.name_expr$")]
pub fn to_lowercase(_: JNIEnv, _: JClass, expr_ptr: *mut Expr) -> jlong {
    let l_expr = from_ptr(expr_ptr);
    let expr = l_expr.name().to_lowercase();
    to_ptr(expr)
}
