#![allow(non_snake_case)]

use jni::objects::ReleaseMode::NoCopyBack;
use jni::objects::{JLongArray, JObject, JObjectArray, JString};
use jni::sys::{jboolean, jlong, jstring, JNI_TRUE};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::export::num::ToPrimitive;
use polars::prelude::*;

use crate::internal_jni::utils::*;
use crate::j_expr::JExpr;
use crate::j_lazy_frame::JLazyFrame;

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn schemaString(mut _env: JNIEnv, _object: JObject, ldf_ptr: jlong) -> jstring {
    let j_ldf = unsafe { &mut *(ldf_ptr as *mut JLazyFrame) };
    let schema_string = serde_json::to_string(&j_ldf.ldf.schema().unwrap()).unwrap();

    to_jstring(
        &mut _env,
        schema_string,
        "Unable to get/ convert Schema to UTF8.",
    )
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn selectFromStrings(
    mut _env: JNIEnv,
    _object: JObject,
    ptr: jlong,
    expr_strs: JObjectArray,
) -> jlong {
    let j_ldf = unsafe { &mut *(ptr as *mut JLazyFrame) };
    let num_expr = _env.get_array_length(&expr_strs).unwrap();

    let mut exprs: Vec<Expr> = Vec::new();

    for i in 0..num_expr {
        let result = _env
            .get_object_array_element(&expr_strs, i)
            .map(JString::from)
            .unwrap();
        let expr_str = get_string(&mut _env, result, "Unable to get/ convert Expr to UTF8.");

        exprs.push(col(expr_str.as_str()))
    }

    j_ldf.select(&mut _env, _object, exprs)
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn selectFromExprs(mut env: JNIEnv, object: JObject, ptr: jlong, inputs: JLongArray) -> jlong {
    let j_ldf = unsafe { &mut *(ptr as *mut JLazyFrame) };

    let arr = unsafe { env.get_array_elements(&inputs, NoCopyBack).unwrap() };
    let exprs: Vec<Expr> = unsafe {
        std::slice::from_raw_parts(arr.as_ptr(), arr.len())
            .to_vec()
            .iter()
            .map(|p| p.to_i64().unwrap())
            .map(|ptr| {
                let j_ldf = &mut *(ptr as *mut JExpr);
                j_ldf.to_owned().expr
            })
            .collect()
    };

    j_ldf.select(&mut env, object, exprs)
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn filterFromExprs(mut env: JNIEnv, object: JObject, ldf_ptr: jlong, expr_ptr: jlong) -> jlong {
    let j_ldf = unsafe { &mut *(ldf_ptr as *mut JLazyFrame) };
    let j_expr = unsafe { &mut *(expr_ptr as *mut JExpr) };

    j_ldf.filter(&mut env, object, j_expr.expr.clone())
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn sortFromExprs(
    mut env: JNIEnv,
    object: JObject,
    ldf_ptr: jlong,
    inputs: JLongArray,
    nullLast: jboolean,
    maintainOrder: jboolean,
) -> jlong {
    let j_ldf = unsafe { &mut *(ldf_ptr as *mut JLazyFrame) };

    let arr = unsafe { env.get_array_elements(&inputs, NoCopyBack).unwrap() };
    let exprs: Vec<Expr> = unsafe {
        std::slice::from_raw_parts(arr.as_ptr(), arr.len())
            .to_vec()
            .iter()
            .map(|p| p.to_i64().unwrap())
            .map(|ptr| {
                let j_ldf = &mut *(ptr as *mut JExpr);
                j_ldf.to_owned().expr
            })
            .collect()
    };

    j_ldf.sort(
        &mut env,
        object,
        exprs,
        nullLast == JNI_TRUE,
        maintainOrder == JNI_TRUE,
    )
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn withColumn(
    mut env: JNIEnv,
    object: JObject,
    ldf_ptr: jlong,
    col_name: JString,
    expr_ptr: jlong,
) -> jlong {
    let j_ldf = unsafe { &mut *(ldf_ptr as *mut JLazyFrame) };
    let name = get_string(&mut env, col_name, "Unable to get/ convert value to UTF8.");
    let j_expr = unsafe { &mut *(expr_ptr as *mut JExpr) };

    let ldf = j_ldf
        .ldf
        .clone()
        .with_column(j_expr.expr.clone().alias(name.as_str()));

    ldf_to_ptr(&mut env, object, Ok(ldf))
}

#[allow(clippy::too_many_arguments)]
#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn optimization_toggle(
    mut env: JNIEnv,
    object: JObject,
    ptr: jlong,
    typeCoercion: jboolean,
    predicatePushdown: jboolean,
    projectionPushdown: jboolean,
    simplifyExpr: jboolean,
    slicePushdown: jboolean,
    commSubplanElim: jboolean,
    commSubexprElim: jboolean,
    streaming: jboolean,
) -> jlong {
    let j_ldf = unsafe { &mut *(ptr as *mut JLazyFrame) };
    j_ldf.optimization_toggle(
        &mut env,
        object,
        typeCoercion == JNI_TRUE,
        predicatePushdown == JNI_TRUE,
        projectionPushdown == JNI_TRUE,
        simplifyExpr == JNI_TRUE,
        slicePushdown == JNI_TRUE,
        commSubplanElim == JNI_TRUE,
        commSubexprElim == JNI_TRUE,
        streaming == JNI_TRUE,
    )
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn cache(mut env: JNIEnv, object: JObject, ptr: jlong) -> jlong {
    let j_ldf = unsafe { &mut *(ptr as *mut JLazyFrame) };
    j_ldf.cache(&mut env, object)
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn collect(mut env: JNIEnv, object: JObject, ptr: jlong) -> jlong {
    let j_ldf = unsafe { &mut *(ptr as *mut JLazyFrame) };
    j_ldf.collect(&mut env, object)
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn concatLazyFrames(
    mut env: JNIEnv,
    object: JObject,
    inputs: JLongArray,
    parallel: jboolean,
    re_chunk: jboolean,
) -> jlong {
    let arr = unsafe { env.get_array_elements(&inputs, NoCopyBack).unwrap() };

    let vec: Vec<LazyFrame> = unsafe {
        std::slice::from_raw_parts(arr.as_ptr(), arr.len())
            .to_vec()
            .iter()
            .map(|p| p.to_i64().unwrap())
            .map(|ptr| {
                let j_ldf = &mut *(ptr as *mut JLazyFrame);
                j_ldf.to_owned().ldf
            })
            .collect()
    };

    let concat_ldf = concat(
        vec,
        UnionArgs {
            rechunk: re_chunk == JNI_TRUE,
            parallel: parallel == JNI_TRUE,
            ..Default::default()
        },
    );
    ldf_to_ptr(&mut env, object, concat_ldf)
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn limit(mut env: JNIEnv, object: JObject, ldf_ptr: jlong, n: jlong) -> jlong {
    let j_ldf = unsafe { &mut *(ldf_ptr as *mut JLazyFrame) };

    j_ldf.limit(&mut env, object, n as IdxSize)
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn tail(mut env: JNIEnv, object: JObject, ldf_ptr: jlong, n: jlong) -> jlong {
    let j_ldf = unsafe { &mut *(ldf_ptr as *mut JLazyFrame) };

    j_ldf.tail(&mut env, object, n as IdxSize)
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn drop(mut _env: JNIEnv, _object: JObject, ptr: jlong, col_names: JObjectArray) -> jlong {
    let j_ldf = unsafe { &mut *(ptr as *mut JLazyFrame) };
    let num_expr = _env.get_array_length(&col_names).unwrap();

    let mut cols: Vec<String> = Vec::new();

    for i in 0..num_expr {
        let result = _env
            .get_object_array_element(&col_names, i)
            .map(JString::from)
            .unwrap();
        let col_name = get_string(&mut _env, result, "Unable to get/ convert Expr to UTF8.");

        cols.push(col_name)
    }

    j_ldf.drop(&mut _env, _object, cols)
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn rename(mut _env: JNIEnv, object: JObject, ldf_ptr: jlong, options: JObject) -> jlong {
    let j_ldf = unsafe { &mut *(ldf_ptr as *mut JLazyFrame) };

    let opts = _env
        .get_map(&options)
        .expect("Unable to get provided mapping.");

    let mut iterator = opts
        .iter(&mut _env)
        .expect("The provided mapping are not iterable.");

    let mut old_vec: Vec<String> = Vec::new();
    let mut new_vec: Vec<String> = Vec::new();

    while let Ok(Some((new, old))) = iterator.next(&mut _env) {
        let key_str: String = _env
            .get_string(&JString::from(new))
            .expect("Invalid old column name.")
            .into();

        let value_str: String = _env
            .get_string(&JString::from(old))
            .expect("Invalid new column name.")
            .into();

        old_vec.push(key_str);
        new_vec.push(value_str);
    }

    j_ldf.rename(&mut _env, object, old_vec, new_vec)
}

#[jni_fn("org.polars.scala.polars.internal.jni.lazy_frame$")]
pub fn explain(mut _env: JNIEnv, _object: JObject, ldf_ptr: jlong, optimized: jboolean) -> jstring {
    let j_ldf = unsafe { &mut *(ldf_ptr as *mut JLazyFrame) };
    let plan_str = if optimized == JNI_TRUE {
        j_ldf.ldf.describe_optimized_plan()
    } else {
        Ok(j_ldf.ldf.describe_plan())
    }
    .expect("Unable to describe plan.");

    to_jstring(&mut _env, plan_str, "Unable to get/ convert plan to UTF8.")
}
