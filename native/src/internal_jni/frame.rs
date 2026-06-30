use std::iter::Iterator;

use anyhow::Context;
use jni::objects::{JLongArray, JObject, JString};
use jni::sys::jlong;
use jni::{Env, NativeMethod, native_method};
use polars::prelude::*;
use polars_core::utils::concat_df;

use crate::internal_jni::conversion::JavaArrayToVec;
use crate::internal_jni::handle::{DataFrameHandle, Handle, LazyFrameHandle, SeriesHandle};
use crate::utils::error::ThrowRuntimeException;

const SCHEMA_STRING_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.data_frame$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe DataFrameHandle => long },
    extern fn schema_string(df: DataFrameHandle) -> JString,
    name = "schemaString",
};

fn schema_string<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    df: DataFrameHandle,
) -> anyhow::Result<JString<'local>> {
    let df = df.get();
    let schema_str = serde_json::to_string(&df.schema().to_arrow(CompatLevel::oldest()))
        .context("Failed to serialize schema")?;
    JString::from_str(env, schema_str).context("Failed to build schema string")
}

const SHOW_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.data_frame$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe DataFrameHandle => long },
    extern fn show(df: DataFrameHandle),
};

fn show<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    df: DataFrameHandle,
) -> anyhow::Result<()> {
    let df = df.get();
    println!("{df:?}");
    Ok(())
}

const COUNT_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.data_frame$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe DataFrameHandle => long },
    extern fn count(df: DataFrameHandle) -> jlong,
};

fn count<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    df: DataFrameHandle,
) -> anyhow::Result<jlong> {
    Ok(df.get().shape().0 as i64)
}

const CONCAT_DATA_FRAMES_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.data_frame$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe DataFrameHandle => long },
    extern fn concat_data_frames(inputs: [jlong]) -> DataFrameHandle,
    name = "concatDataFrames",
};

fn concat_data_frames<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    inputs: JLongArray<'local>,
) -> anyhow::Result<DataFrameHandle> {
    let dfs: Vec<_> = JavaArrayToVec::to_vec(env, inputs)?
        .into_iter()
        .map(|ptr| DataFrameHandle::from(ptr).get())
        .collect();

    let concatenated_df = concat_df(dfs.iter()).context("Failed to concatenate dataframes")?;

    Ok(DataFrameHandle::alloc(concatenated_df))
}

const TO_LAZY_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.data_frame$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe DataFrameHandle => long, unsafe LazyFrameHandle => long },
    extern fn to_lazy(df: DataFrameHandle) -> LazyFrameHandle,
    name = "toLazy",
};

fn to_lazy<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    df: DataFrameHandle,
) -> anyhow::Result<LazyFrameHandle> {
    Ok(LazyFrameHandle::alloc(df.get().lazy()))
}

const LIMIT_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.data_frame$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe DataFrameHandle => long },
    extern fn limit(df: DataFrameHandle, n: jlong) -> DataFrameHandle,
};

fn limit<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    df: DataFrameHandle,
    n: jlong,
) -> anyhow::Result<DataFrameHandle> {
    Ok(DataFrameHandle::alloc(df.get().head(Some(n as usize))))
}

const TAIL_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.data_frame$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe DataFrameHandle => long },
    extern fn tail(df: DataFrameHandle, n: jlong) -> DataFrameHandle,
};

fn tail<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    df: DataFrameHandle,
    n: jlong,
) -> anyhow::Result<DataFrameHandle> {
    Ok(DataFrameHandle::alloc(df.get().tail(Some(n as usize))))
}

const FROM_SERIES_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.data_frame$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe DataFrameHandle => long },
    extern fn from_series(ptrs: [jlong]) -> DataFrameHandle,
    name = "fromSeries",
};

fn from_series<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ptrs: JLongArray<'local>,
) -> anyhow::Result<DataFrameHandle> {
    let data: Vec<Column> = JavaArrayToVec::to_vec(env, ptrs)?
        .into_iter()
        .map(|ptr| SeriesHandle::from(ptr).get().into_column())
        .collect();

    let df = DataFrame::new_infer_height(data)
        .context("Failed to instantiate DataFrame from the provided series")?;

    Ok(DataFrameHandle::alloc(df))
}

const FREE_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.data_frame$",
    error_policy = ThrowRuntimeException,
    extern fn free(ptr: jlong),
};

fn free<'local>(_env: &mut Env<'local>, _this: JObject<'local>, ptr: jlong) -> anyhow::Result<()> {
    DataFrameHandle::free_raw(ptr);
    Ok(())
}

/// All native methods exported by this module.
pub const METHODS: &[NativeMethod] = &[
    SCHEMA_STRING_METHOD,
    SHOW_METHOD,
    COUNT_METHOD,
    CONCAT_DATA_FRAMES_METHOD,
    TO_LAZY_METHOD,
    LIMIT_METHOD,
    TAIL_METHOD,
    FROM_SERIES_METHOD,
    FREE_METHOD,
];
