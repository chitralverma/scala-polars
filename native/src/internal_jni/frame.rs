#![allow(non_snake_case)]
use std::borrow::ToOwned;
use std::iter::Iterator;

use anyhow::Context;
use jni::objects::{JClass, JLongArray};
use jni::sys::{jlong, jstring};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::prelude::*;
use polars_core::utils::concat_df;

use crate::internal_jni::utils::*;
use crate::utils::error::ResultExt;

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub unsafe fn schemaString(mut env: JNIEnv, _: JClass, df_ptr: *mut DataFrame) -> jstring {
    let df = &mut *df_ptr;

    serde_json::to_string(&df.schema().to_arrow(CompatLevel::oldest()))
        .map(|schema_string| string_to_j_string(&mut env, schema_string, None::<&str>))
        .context("Failed to serialize schema")
        .unwrap_or_throw(&mut env)
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub unsafe fn show(_: JNIEnv, _: JClass, df_ptr: *mut DataFrame) {
    let df = &mut *df_ptr;
    println!("{df:?}")
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub unsafe fn count(_: JNIEnv, _: JClass, df_ptr: *mut DataFrame) -> jlong {
    (*df_ptr).shape().0 as i64
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub unsafe fn concatDataFrames(mut env: JNIEnv, _: JClass, inputs: JLongArray) -> jlong {
    let dfs: Vec<_> = JavaArrayToVec::to_vec(&mut env, inputs)
        .into_iter()
        .map(|ptr| (*(ptr as *mut DataFrame)).to_owned())
        .collect();

    let concatenated_df = concat_df(dfs.iter())
        .context("Failed to concatenate dataframes")
        .unwrap_or_throw(&mut env);

    to_ptr(concatenated_df)
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub unsafe fn toLazy(_: JNIEnv, _: JClass, df_ptr: *mut DataFrame) -> jlong {
    let ldf = (*df_ptr).clone().lazy();
    to_ptr(ldf)
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub unsafe fn limit(_: JNIEnv, _: JClass, df_ptr: *mut DataFrame, n: jlong) -> jlong {
    let limited_df = (*df_ptr).head(Some(n as usize));
    to_ptr(limited_df)
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub unsafe fn tail(_: JNIEnv, _: JClass, df_ptr: *mut DataFrame, n: jlong) -> jlong {
    let limited_df = (*df_ptr).tail(Some(n as usize));
    to_ptr(limited_df)
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub unsafe fn fromSeries(mut env: JNIEnv, _: JClass, ptrs: JLongArray) -> jlong {
    let data: Vec<_> = JavaArrayToVec::to_vec(&mut env, ptrs)
        .into_iter()
        .map(|ptr| (*(ptr as *mut Series)).to_owned())
        .collect();

    let df = DataFrame::from_iter(data);
    to_ptr(df)
}
