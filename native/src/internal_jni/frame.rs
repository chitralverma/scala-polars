#![allow(non_snake_case)]

use jni::objects::ReleaseMode::NoCopyBack;
use jni::objects::{JObject, JString};
use jni::sys::{jlong, jlongArray, jobjectArray, jstring};
use jni::JNIEnv;

use polars::export::num::ToPrimitive;
use polars::prelude::*;
use polars_core::utils::concat_df;

use crate::internal_jni::utils::*;
use crate::j_data_frame::JDataFrame;
use crate::j_expr::JExpr;

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_data_1frame_00024_schemaString(
    env: JNIEnv,
    _object: JObject,
    ldf_ptr: jlong) -> jstring {
    let j_df = unsafe { &mut *(ldf_ptr as *mut JDataFrame) };
    let schema_string = serde_json::to_string(&j_df.df.schema()).unwrap();

    env.new_string(schema_string)
        .expect("Unable to get/ convert Schema to UTF8.")
        .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_data_1frame_00024_selectFromStrings(
    _env: JNIEnv,
    _object: JObject,
    ptr: jlong,
    expr_strs: jobjectArray,
) -> jlong {
    let j_df = unsafe { &mut *(ptr as *mut JDataFrame) };
    let num_expr = _env.get_array_length(expr_strs).unwrap();

    let mut exprs: Vec<Expr> = Vec::new();

    for i in 0..num_expr {
        let result = _env
            .get_object_array_element(expr_strs, i)
            .map(JString::from)
            .unwrap();
        let expr_str = get_string(_env, result, "Unable to get/ convert Expr to UTF8.");

        exprs.push(col(expr_str.as_str()))
    }

    j_df.select(_env, _object, exprs)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_data_1frame_00024_selectFromExprs(
    env: JNIEnv,
    object: JObject,
    ptr: jlong,
    inputs: jlongArray,
) -> jlong {
    let j_df = unsafe { &mut *(ptr as *mut JDataFrame) };

    let arr = env.get_long_array_elements(inputs, NoCopyBack).unwrap();
    let exprs: Vec<Expr> = unsafe {
        std::slice::from_raw_parts(arr.as_ptr(), arr.size().unwrap() as usize)
            .to_vec()
            .iter()
            .map(|p| p.to_i64().unwrap())
            .map(|ptr| {
                let j_ldf = &mut *(ptr as *mut JExpr);
                j_ldf.to_owned().expr
            })
            .collect()
    };

    j_df.select(env, object, exprs)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_data_1frame_00024_filterFromExprs(
    env: JNIEnv,
    object: JObject,
    ldf_ptr: jlong,
    expr_ptr: jlong,
) -> jlong {
    let j_df = unsafe { &mut *(ldf_ptr as *mut JDataFrame) };
    let j_expr = unsafe { &mut *(expr_ptr as *mut JExpr) };

    j_df.filter(env, object, j_expr.expr.clone())
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_data_1frame_00024_show(
    _env: JNIEnv,
    _object: JObject,
    ptr: jlong,
) {
    let j_df = unsafe { &mut *(ptr as *mut JDataFrame) };
    j_df.show()
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_data_1frame_00024_count(
    _env: JNIEnv,
    _object: JObject,
    ptr: jlong,
) -> jlong {
    let j_df = unsafe { &mut *(ptr as *mut JDataFrame) };

    j_df.df.shape().0 as i64
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_common_00024__1concatDataFrames(
    env: JNIEnv,
    object: JObject,
    inputs: jlongArray,
) -> jlong {
    let arr = env.get_long_array_elements(inputs, NoCopyBack).unwrap();

    let vec: Vec<DataFrame> = unsafe {
        std::slice::from_raw_parts(arr.as_ptr(), arr.size().unwrap() as usize)
            .to_vec()
            .iter()
            .map(|p| p.to_i64().unwrap())
            .map(|ptr| {
                let j_ldf = &mut *(ptr as *mut JDataFrame);
                j_ldf.to_owned().df
            })
            .collect()
    };

    let concat_ldf = concat_df(vec.as_slice());
    df_to_ptr(env, object, concat_ldf)
}
