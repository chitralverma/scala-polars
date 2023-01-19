#![allow(non_snake_case)]

use jni::objects::{JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use polars::prelude::*;

use crate::internal_jni::utils::{expr_to_ptr, get_string};
use crate::j_expr::JExpr;

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_column(
    env: JNIEnv,
    object: JObject,
    col_name: JString,
) -> jlong {
    let name = get_string(env, col_name, "Unable to get/ convert column name to UTF8.");

    let expr = col(name.as_str());
    expr_to_ptr(env, object, expr)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_not(
    env: JNIEnv,
    object: JObject,
    ptr: jlong,
) -> jlong {
    let expr = unsafe { &mut *(ptr as *mut JExpr) };

    expr.not(env, object)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_and(
    env: JNIEnv,
    object: JObject,
    left_ptr: jlong,
    right_ptr: jlong,
) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.and(env, object, right_expr.to_owned())
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_or(
    env: JNIEnv,
    object: JObject,
    left_ptr: jlong,
    right_ptr: jlong,
) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.or(env, object, right_expr.to_owned())
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_equalTo(
    env: JNIEnv,
    object: JObject,
    left_ptr: jlong,
    right_ptr: jlong,
) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.equal_to(env, object, right_expr.to_owned())
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_notEqualTo(
    env: JNIEnv,
    object: JObject,
    left_ptr: jlong,
    right_ptr: jlong,
) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.not_equal_to(env, object, right_expr.to_owned())
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_lessThan(
    env: JNIEnv,
    object: JObject,
    left_ptr: jlong,
    right_ptr: jlong,
) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.less_than(env, object, right_expr.to_owned())
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_greaterThan(
    env: JNIEnv,
    object: JObject,
    left_ptr: jlong,
    right_ptr: jlong,
) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.greater_than(env, object, right_expr.to_owned())
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_lessThanEqualTo(
    env: JNIEnv,
    object: JObject,
    left_ptr: jlong,
    right_ptr: jlong,
) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.less_than_equal_to(env, object, right_expr.to_owned())
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_greaterThanEqualTo(
    env: JNIEnv,
    object: JObject,
    left_ptr: jlong,
    right_ptr: jlong,
) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    left_expr.greater_than_equal_to(env, object, right_expr.to_owned())
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_isNull(
    env: JNIEnv,
    object: JObject,
    ptr: jlong,
) -> jlong {
    let expr = unsafe { &mut *(ptr as *mut JExpr) };

    expr.is_null(env, object)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_isNotNull(
    env: JNIEnv,
    object: JObject,
    ptr: jlong,
) -> jlong {
    let expr = unsafe { &mut *(ptr as *mut JExpr) };

    expr.is_not_null(env, object)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_isNaN(
    env: JNIEnv,
    object: JObject,
    ptr: jlong,
) -> jlong {
    let expr = unsafe { &mut *(ptr as *mut JExpr) };

    expr.is_nan(env, object)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_expressions_column_1expr_00024_isNotNaN(
    env: JNIEnv,
    object: JObject,
    ptr: jlong,
) -> jlong {
    let expr = unsafe { &mut *(ptr as *mut JExpr) };

    expr.is_not_nan(env, object)
}
