#![allow(non_snake_case)]

use jni::objects::ReleaseMode::NoCopyBack;
use jni::objects::{JObject, JString};
use jni::sys::{jboolean, jchar, jint, jlong, jlongArray, jobjectArray, JNI_TRUE, jstring};
use jni::JNIEnv;
use polars::export::num::ToPrimitive;
use polars::prelude::*;

use crate::internal_jni::utils::*;
use crate::j_expr::JExpr;
use crate::j_lazy_frame::JLazyFrame;

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_lazy_1frame_00024_schemaString(
    env: JNIEnv,
    _object: JObject,
    ldf_ptr: jlong) -> jstring {
    let j_ldf = unsafe { &mut *(ldf_ptr as *mut JLazyFrame) };
    let schema_string = serde_json::to_string(&j_ldf.ldf.schema().unwrap()).unwrap();

    env.new_string(schema_string)
        .expect("Unable to get/ convert Schema to UTF8.")
        .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_lazy_1frame_00024_selectFromStrings(
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

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_lazy_1frame_00024_selectFromExprs(
    env: JNIEnv,
    object: JObject,
    ptr: jlong,
    inputs: jlongArray,
) -> jlong {
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

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_lazy_1frame_00024_filterFromExprs(
    env: JNIEnv,
    object: JObject,
    ldf_ptr: jlong,
    expr_ptr: jlong,
) -> jlong {
    let j_ldf = unsafe { &mut *(ldf_ptr as *mut JLazyFrame) };
    let j_expr = unsafe { &mut *(expr_ptr as *mut JExpr) };

    j_ldf.filter(env, object, j_expr.expr.clone())
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_lazy_1frame_00024_collect(
    env: JNIEnv,
    object: JObject,
    ptr: jlong,
) -> jlong {
    let j_ldf = unsafe { &mut *(ptr as *mut JLazyFrame) };
    j_ldf.collect(env, object)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_common_00024__1concatLazyFrames(
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

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_io_parquet_00024__1scanParquet(
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
    };

    let j_ldf = LazyFrame::scan_parquet(this_path, scan_args);
    ldf_to_ptr(env, object, j_ldf)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_io_csv_00024__1scanCSV(
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
        .with_ignore_parser_errors(ignoreErrors == JNI_TRUE)
        .with_row_count(row_count)
        .with_infer_schema_length(Some(inferSchemaRows as usize))
        .with_parse_dates(parseDates == JNI_TRUE)
        .with_cache(cache == JNI_TRUE)
        .with_rechunk(reChunk == JNI_TRUE)
        .low_memory(lowMemory == JNI_TRUE)
        .finish();

    ldf_to_ptr(env, object, j_ldf)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_io_ndjson_00024__1scanNdJson(
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

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_io_ipc_00024__1scanIPC(
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
