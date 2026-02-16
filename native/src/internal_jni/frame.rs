use std::iter::Iterator;

use anyhow::Context;
use jni::JNIEnv;
use jni::objects::{JClass, JLongArray};
use jni::sys::{jlong, jstring};
use jni_fn::jni_fn;
use polars::prelude::*;
use polars_core::utils::concat_df;

use crate::internal_jni::utils::*;
use crate::internal_jni::conversion::JavaArrayToVec;
use crate::utils::error::ResultExt;

#[jni_fn("com.github.chitralverma.polars.internal.jni.data_frame$")]
pub fn schemaString(mut env: JNIEnv, _: JClass, df_ptr: *mut DataFrame) -> jstring {
    let df = &mut from_ptr(df_ptr);

    serde_json::to_string(&df.schema().to_arrow(CompatLevel::oldest()))
        .map(|schema_string| string_to_j_string(&mut env, schema_string, None::<&str>))
        .context("Failed to serialize schema")
        .unwrap_or_throw(&mut env)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.data_frame$")]
pub fn show(_: JNIEnv, _: JClass, df_ptr: *mut DataFrame) {
    let df = &mut from_ptr(df_ptr);
    println!("{df:?}")
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.data_frame$")]
pub fn count(_: JNIEnv, _: JClass, df_ptr: *mut DataFrame) -> jlong {
    from_ptr(df_ptr).shape().0 as i64
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.data_frame$")]
pub fn concatDataFrames(mut env: JNIEnv, _: JClass, inputs: JLongArray) -> jlong {
    let dfs: Vec<_> = JavaArrayToVec::to_vec(&mut env, inputs)
        .into_iter()
        .map(|ptr| from_ptr(ptr as *mut DataFrame))
        .collect();

    let concatenated_df = concat_df(dfs.iter())
        .context("Failed to concatenate dataframes")
        .unwrap_or_throw(&mut env);

    to_ptr(concatenated_df)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.data_frame$")]
pub fn toLazy(_: JNIEnv, _: JClass, df_ptr: *mut DataFrame) -> jlong {
    let ldf = from_ptr(df_ptr).lazy();
    to_ptr(ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.data_frame$")]
pub fn limit(_: JNIEnv, _: JClass, df_ptr: *mut DataFrame, n: jlong) -> jlong {
    let limited_df = from_ptr(df_ptr).head(Some(n as usize));
    to_ptr(limited_df)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.data_frame$")]
pub fn tail(_: JNIEnv, _: JClass, df_ptr: *mut DataFrame, n: jlong) -> jlong {
    let limited_df = from_ptr(df_ptr).tail(Some(n as usize));
    to_ptr(limited_df)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.data_frame$")]
pub fn fromSeries(mut env: JNIEnv, _: JClass, ptrs: JLongArray) -> jlong {
    let data: Vec<Column> = JavaArrayToVec::to_vec(&mut env, ptrs)
        .into_iter()
        .map(|ptr| from_ptr(ptr as *mut Series).into_column())
        .collect();

    let df = DataFrame::new_infer_height(data)
        .context("Failed to instantiate DataFrame from the provided series")
        .unwrap_or_throw(&mut env);

    to_ptr(df)
}
