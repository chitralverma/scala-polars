#![allow(non_snake_case)]

use jni::objects::ReleaseMode::NoCopyBack;
use jni::objects::{JObject, JString};
use jni::sys::{jboolean, jlong, jlongArray, jobjectArray, jstring, JNI_TRUE};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::export::num::ToPrimitive;
use polars::prelude::*;

use crate::internal_jni::utils::*;
use crate::j_expr::JExpr;
use crate::j_lazy_frame::JLazyFrame;

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn schemaString(env: JNIEnv, _object: JObject, ldf_ptr: jlong) -> jstring {
    let j_ldf = unsafe { &mut *(ldf_ptr as *mut JLazyFrame) };
    let schema_string = serde_json::to_string(&j_ldf.ldf.schema().unwrap()).unwrap();

    env.new_string(schema_string)
        .expect("Unable to get/ convert Schema to UTF8.")
        .into_raw()
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn selectFromStrings(
    _env: JNIEnv,
    _object: JObject,
    ptr: jlong,
    expr_strs: jobjectArray,
) -> jlong {
    let j_ldf = unsafe { &mut *(ptr as *mut JLazyFrame) };
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

    j_ldf.select(_env, _object, exprs)
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn selectFromExprs(env: JNIEnv, object: JObject, ptr: jlong, inputs: jlongArray) -> jlong {
    let j_ldf = unsafe { &mut *(ptr as *mut JLazyFrame) };

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

    j_ldf.select(env, object, exprs)
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn filterFromExprs(env: JNIEnv, object: JObject, ldf_ptr: jlong, expr_ptr: jlong) -> jlong {
    let j_ldf = unsafe { &mut *(ldf_ptr as *mut JLazyFrame) };
    let j_expr = unsafe { &mut *(expr_ptr as *mut JExpr) };

    j_ldf.filter(env, object, j_expr.expr.clone())
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn withColumn(
    env: JNIEnv,
    object: JObject,
    ldf_ptr: jlong,
    col_name: JString,
    expr_ptr: jlong,
) -> jlong {
    let j_ldf = unsafe { &mut *(ldf_ptr as *mut JLazyFrame) };
    let name = get_string(env, col_name, "Unable to get/ convert value to UTF8.");
    let j_expr = unsafe { &mut *(expr_ptr as *mut JExpr) };

    let ldf = j_ldf
        .ldf
        .clone()
        .with_column(j_expr.expr.clone().alias(name.as_str()));

    ldf_to_ptr(env, object, Ok(ldf))
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn collect(env: JNIEnv, object: JObject, ptr: jlong) -> jlong {
    let j_ldf = unsafe { &mut *(ptr as *mut JLazyFrame) };
    j_ldf.collect(env, object)
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn concatLazyFrames(
    env: JNIEnv,
    object: JObject,
    inputs: jlongArray,
    parallel: jboolean,
    re_chunk: jboolean,
) -> jlong {
    let arr = env.get_long_array_elements(inputs, NoCopyBack).unwrap();

    let vec: Vec<LazyFrame> = unsafe {
        std::slice::from_raw_parts(arr.as_ptr(), arr.size().unwrap() as usize)
            .to_vec()
            .iter()
            .map(|p| p.to_i64().unwrap())
            .map(|ptr| {
                let j_ldf = &mut *(ptr as *mut JLazyFrame);
                j_ldf.to_owned().ldf
            })
            .collect()
    };

    let concat_ldf = concat(vec, re_chunk == JNI_TRUE, parallel == JNI_TRUE);
    ldf_to_ptr(env, object, concat_ldf)
}
