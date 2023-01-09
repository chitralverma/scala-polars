#![allow(non_snake_case)]

use jni::objects::ReleaseMode::NoCopyBack;
use jni::objects::{JObject, JString};
use jni::sys::{jboolean, jchar, jint, jlong, jlongArray, JNI_TRUE};
use jni::JNIEnv;
use polars::export::num::ToPrimitive;
use polars::prelude::*;

use crate::internal_jni::utils::*;
use crate::j_lazy_frame::JLazyFrame;

#[no_mangle]
pub unsafe extern "system" fn Java_org_polars_scala_polars_internal_jni_lazy_1frame_00024_collect(
    _env: JNIEnv,
    _object: JObject,
    ptr: jlong,
) -> jlong {
    let j_ldf = &mut *(ptr as *mut JLazyFrame);
    j_ldf.collect(_env, _object)
}

#[no_mangle]
pub unsafe extern "system" fn Java_org_polars_scala_polars_internal_jni_common_00024__1concatLazyFrames(
    env: JNIEnv,
    object: JObject,
    inputs: jlongArray,
) -> jlong {
    let arr = env.get_long_array_elements(inputs, NoCopyBack).unwrap();

    let vec: Vec<LazyFrame> =
        std::slice::from_raw_parts(arr.as_ptr(), arr.size().unwrap() as usize)
            .to_vec()
            .iter()
            .map(|p| p.to_i64().unwrap())
            .map(|ptr| {
                let j_ldf = &mut *(ptr as *mut JLazyFrame);
                j_ldf.to_owned().ldf
            })
            .collect();

    let concat_ldf = concat(vec, false, true);
    ldf_to_ptr(env, object, concat_ldf)
}

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_io_parquet_00024__1scanParquet(
    env: JNIEnv,
    object: JObject,
    filePath: JString,
    nRows: jlong,
    rowCountColName: JString,
    rowCountColOffset: jint,
) -> jlong {
    let this_path = get_file_path(env, filePath);
    let n_rows = get_n_rows(nRows);
    let row_count = get_row_count(env, rowCountColName, rowCountColOffset);

    let scan_args = ScanArgsParquet {
        n_rows,
        cache: false,
        parallel: Default::default(),
        rechunk: false,
        row_count,
        low_memory: false,
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
        .finish();

    ldf_to_ptr(env, object, j_ldf)
}
