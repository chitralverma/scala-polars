#![allow(non_snake_case)]

use jni::objects::{JObject, JString};
use jni::JNIEnv;

use jni::sys::{jboolean, jchar, jint, jlong, JNI_TRUE};
use jni_fn::jni_fn;

use polars::prelude::*;

use crate::internal_jni::utils::*;

#[jni_fn("org.polars.scala.polars.internal.jni.io.scan$")]
pub fn scanParquet(
    env: JNIEnv,
    object: JObject,
    filePath: JString,
    nRows: jlong,
    cache: jboolean,
    reChunk: jboolean,
    lowMemory: jboolean,
    rowCountColName: JString,
    rowCountColOffset: jint,
) -> jlong {
    let this_path = get_file_path(env, filePath);
    let n_rows = get_n_rows(nRows);
    let row_count = get_row_count(env, rowCountColName, rowCountColOffset);

    let scan_args = ScanArgsParquet {
        n_rows,
        row_count,
        parallel: Default::default(),
        cache: cache == JNI_TRUE,
        rechunk: reChunk == JNI_TRUE,
        low_memory: lowMemory == JNI_TRUE,
        cloud_options: None,
    };

    let j_ldf = LazyFrame::scan_parquet(this_path, scan_args);
    ldf_to_ptr(env, object, j_ldf)
}

#[jni_fn("org.polars.scala.polars.internal.jni.io.scan$")]
pub fn scanCSV(
    env: JNIEnv,
    object: JObject,
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
    let this_path = get_file_path(env, filePath);
    let n_rows = get_n_rows(nRows);
    let row_count = get_row_count(env, rowCountColName, rowCountColOffset);

    let j_ldf = LazyCsvReader::new(this_path)
        .with_n_rows(n_rows)
        .with_delimiter(delimiter as u8)
        .has_header(hasHeader == JNI_TRUE)
        .with_ignore_errors(ignoreErrors == JNI_TRUE)
        .with_row_count(row_count)
        .with_infer_schema_length(Some(inferSchemaRows as usize))
        .with_parse_dates(parseDates == JNI_TRUE)
        .with_cache(cache == JNI_TRUE)
        .with_rechunk(reChunk == JNI_TRUE)
        .low_memory(lowMemory == JNI_TRUE)
        .finish();

    ldf_to_ptr(env, object, j_ldf)
}

#[jni_fn("org.polars.scala.polars.internal.jni.io.scan$")]
pub fn scanNdJson(
    env: JNIEnv,
    object: JObject,
    filePath: JString,
    nRows: jlong,
    inferSchemaRows: jlong,
    cache: jboolean,
    reChunk: jboolean,
    lowMemory: jboolean,
    rowCountColName: JString,
    rowCountColOffset: jint,
) -> jlong {
    let this_path = get_file_path(env, filePath);
    let n_rows = get_n_rows(nRows);
    let row_count = get_row_count(env, rowCountColName, rowCountColOffset);

    let j_ldf = LazyJsonLineReader::new(this_path)
        .with_n_rows(n_rows)
        .with_row_count(row_count)
        .with_infer_schema_length(Some(inferSchemaRows as usize))
        .with_rechunk(reChunk == JNI_TRUE)
        .low_memory(lowMemory == JNI_TRUE)
        .finish();

    let cached_or_not = j_ldf
        .map(|l| if cache == JNI_TRUE { l.cache() } else { l })
        .unwrap();

    ldf_to_ptr(env, object, Ok(cached_or_not))
}

#[jni_fn("org.polars.scala.polars.internal.jni.io.scan$")]
pub fn scanIPC(
    env: JNIEnv,
    object: JObject,
    filePath: JString,
    nRows: jlong,
    cache: jboolean,
    re_chunk: jboolean,
    mem_map: jboolean,
    rowCountColName: JString,
    rowCountColOffset: jint,
) -> jlong {
    let this_path = get_file_path(env, filePath);
    let n_rows = get_n_rows(nRows);
    let row_count = get_row_count(env, rowCountColName, rowCountColOffset);

    let scan_args = ScanArgsIpc {
        n_rows,
        cache: cache == JNI_TRUE,
        rechunk: re_chunk == JNI_TRUE,
        row_count,
        memmap: mem_map == JNI_TRUE,
    };

    let j_ldf = LazyFrame::scan_ipc(this_path, scan_args);
    ldf_to_ptr(env, object, j_ldf)
}
