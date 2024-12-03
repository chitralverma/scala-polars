#![allow(non_snake_case)]

use std::num::NonZeroUsize;

use anyhow::Context;
use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jchar, jint, jlong, JNI_TRUE};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::io::HiveOptions;
use polars::prelude::*;

use super::{get_file_path, get_row_index};
use crate::internal_jni::utils::*;
use crate::utils::error::ResultExt;

#[jni_fn("org.polars.scala.polars.internal.jni.io.scan$")]
pub fn scanParquet(
    mut env: JNIEnv,
    _: JClass,
    filePath: JString,
    nRows: jlong,
    cache: jboolean,
    reChunk: jboolean,
    lowMemory: jboolean,
    hivePartitioning: jboolean,
    rowCountColName: JString,
    rowCountColOffset: jint,
) -> jlong {
    let this_path = get_file_path(&mut env, filePath);
    let n_rows = get_n_rows(nRows);
    let row_index = get_row_index(&mut env, rowCountColName, rowCountColOffset);

    let scan_args = ScanArgsParquet {
        n_rows,
        row_index,
        parallel: Default::default(),
        cache: cache == JNI_TRUE,
        rechunk: reChunk == JNI_TRUE,
        low_memory: lowMemory == JNI_TRUE,
        cloud_options: None,
        use_statistics: true,
        hive_options: HiveOptions {
            enabled: Some(hivePartitioning == JNI_TRUE),
            hive_start_idx: 0,
            try_parse_dates: true,
            schema: None,
        },
        ..Default::default()
    };

    let ldf = LazyFrame::scan_parquet(this_path, scan_args)
        .context("Failed to perform parquet scan")
        .unwrap_or_throw(&mut env);
    to_ptr(ldf)
}

#[jni_fn("org.polars.scala.polars.internal.jni.io.scan$")]
pub fn scanCSV(
    mut env: JNIEnv,
    _: JClass,
    filePath: JString,
    nRows: jlong,
    delimiter: jchar,
    hasHeader: jboolean,
    inferSchemaRows: jlong,
    ignoreErrors: jboolean,
    parseDates: jboolean,
    cache: jboolean,
    reChunk: jboolean,
    lowMemory: jboolean,
    rowCountColName: JString,
    rowCountColOffset: jint,
) -> jlong {
    let this_path = get_file_path(&mut env, filePath);
    let n_rows = get_n_rows(nRows);
    let row_index = get_row_index(&mut env, rowCountColName, rowCountColOffset);

    let ldf = LazyCsvReader::new(this_path)
        .with_n_rows(n_rows)
        .with_separator(delimiter as u8)
        .with_has_header(hasHeader == JNI_TRUE)
        .with_ignore_errors(ignoreErrors == JNI_TRUE)
        .with_row_index(row_index)
        .with_infer_schema_length(Some(inferSchemaRows as usize))
        .with_try_parse_dates(parseDates == JNI_TRUE)
        .with_cache(cache == JNI_TRUE)
        .with_rechunk(reChunk == JNI_TRUE)
        .with_low_memory(lowMemory == JNI_TRUE)
        .finish()
        .context("Failed to perform CSV scan")
        .unwrap_or_throw(&mut env);

    to_ptr(ldf)
}

#[jni_fn("org.polars.scala.polars.internal.jni.io.scan$")]
pub fn scanNdJson(
    mut env: JNIEnv,
    _: JClass,
    filePath: JString,
    nRows: jlong,
    inferSchemaRows: jlong,
    cache: jboolean,
    reChunk: jboolean,
    lowMemory: jboolean,
    rowCountColName: JString,
    rowCountColOffset: jint,
) -> jlong {
    let this_path = get_file_path(&mut env, filePath);
    let n_rows = get_n_rows(nRows);
    let row_index = get_row_index(&mut env, rowCountColName, rowCountColOffset);

    let ldf = LazyJsonLineReader::new(this_path)
        .with_n_rows(n_rows)
        .with_row_index(row_index)
        .with_infer_schema_length(NonZeroUsize::new(inferSchemaRows as usize))
        .with_rechunk(reChunk == JNI_TRUE)
        .low_memory(lowMemory == JNI_TRUE)
        .finish()
        .context("Failed to perform ndjson scan")
        .unwrap_or_throw(&mut env);

    let cached_or_not = if cache == JNI_TRUE { ldf.cache() } else { ldf };

    to_ptr(cached_or_not)
}

#[jni_fn("org.polars.scala.polars.internal.jni.io.scan$")]
pub fn scanIPC(
    mut env: JNIEnv,
    _: JClass,
    filePath: JString,
    nRows: jlong,
    cache: jboolean,
    re_chunk: jboolean,
    rowCountColName: JString,
    rowCountColOffset: jint,
) -> jlong {
    let this_path = get_file_path(&mut env, filePath);
    let n_rows = get_n_rows(nRows);
    let row_index = get_row_index(&mut env, rowCountColName, rowCountColOffset);

    let scan_args = ScanArgsIpc {
        n_rows,
        cache: cache == JNI_TRUE,
        rechunk: re_chunk == JNI_TRUE,
        row_index,
        ..Default::default()
    };

    let ldf = LazyFrame::scan_ipc(this_path, scan_args)
        .context("Failed to perform IPC scan")
        .unwrap_or_throw(&mut env);

    to_ptr(ldf)
}
