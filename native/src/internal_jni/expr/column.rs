#![allow(non_snake_case)]

use jni::objects::{JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::prelude::*;

use crate::internal_jni::utils::{expr_to_ptr, get_string};
use crate::j_expr::JExpr;

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn column(env: JNIEnv, object: JObject, col_name: JString) -> jlong {
    let name = get_string(env, col_name, "Unable to get/ convert column name to UTF8.");

    let expr = col(name.as_str());
    expr_to_ptr(env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn not(env: JNIEnv, object: JObject, ptr: jlong) -> jlong {
    let expr = unsafe { &mut *(ptr as *mut JExpr) };

    expr.not(env, object)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn and(env: JNIEnv, object: JObject, left_ptr: jlong, right_ptr: jlong) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.and(env, object, right_expr.to_owned())
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn or(env: JNIEnv, object: JObject, left_ptr: jlong, right_ptr: jlong) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.or(env, object, right_expr.to_owned())
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn equalTo(env: JNIEnv, object: JObject, left_ptr: jlong, right_ptr: jlong) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.equal_to(env, object, right_expr.to_owned())
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn notEqualTo(env: JNIEnv, object: JObject, left_ptr: jlong, right_ptr: jlong) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.not_equal_to(env, object, right_expr.to_owned())
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn lessThan(env: JNIEnv, object: JObject, left_ptr: jlong, right_ptr: jlong) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.less_than(env, object, right_expr.to_owned())
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn greaterThan(env: JNIEnv, object: JObject, left_ptr: jlong, right_ptr: jlong) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.greater_than(env, object, right_expr.to_owned())
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn lessThanEqualTo(env: JNIEnv, object: JObject, left_ptr: jlong, right_ptr: jlong) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.less_than_equal_to(env, object, right_expr.to_owned())
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn greaterThanEqualTo(
    env: JNIEnv,
    object: JObject,
    left_ptr: jlong,
    right_ptr: jlong,
) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.greater_than_equal_to(env, object, right_expr.to_owned())
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn isNull(env: JNIEnv, object: JObject, ptr: jlong) -> jlong {
    let expr = unsafe { &mut *(ptr as *mut JExpr) };

    expr.is_null(env, object)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn isNotNull(env: JNIEnv, object: JObject, ptr: jlong) -> jlong {
    let expr = unsafe { &mut *(ptr as *mut JExpr) };

    expr.is_not_null(env, object)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn isNaN(env: JNIEnv, object: JObject, ptr: jlong) -> jlong {
    let expr = unsafe { &mut *(ptr as *mut JExpr) };

    expr.is_nan(env, object)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn isNotNaN(env: JNIEnv, object: JObject, ptr: jlong) -> jlong {
    let expr = unsafe { &mut *(ptr as *mut JExpr) };

    expr.is_not_nan(env, object)
}
