#![allow(non_snake_case)]

use jni::objects::{JObject, JString};
use jni::sys::{jboolean, jchar, jint, jlong, JNI_TRUE};
use jni::JNIEnv;

use polars::export::num::ToPrimitive;
use polars::io::RowCount;

use polars::prelude::*;

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
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_io_parquet_00024__1scanParquet(
    env: JNIEnv,
    object: JObject,
    filePath: JString,
    nRows: jlong,
    rowCountColName: JString,
    rowCountColOffset: jint,
) -> jlong {
    let this_path: String = env
        .get_string(filePath)
        .expect("Unable to get/ convert raw path to UTF8.")
        .into();

    let n_rows = if nRows.is_positive() {
        nRows.to_usize()
    } else {
        None
    };

    let row_count = if !rowCountColName.is_null() {
        Some(RowCount {
            name: env
                .get_string(rowCountColName)
                .expect("Unable to get/ convert row column name to UTF8.")
                .into(),
            offset: if rowCountColOffset.is_positive() {
                rowCountColOffset as IdxSize
            } else {
                0
            },
        })
    } else {
        None
    };

    let scan_args = ScanArgsParquet {
        n_rows,
        cache: false,
        parallel: Default::default(),
        rechunk: false,
        row_count,
        low_memory: false,
    };

    let ldf = LazyFrame::scan_parquet(this_path, scan_args)
        .expect("Cannot create LazyFrame from provided arguments.");

    let global_ref = env.new_global_ref(object).unwrap();
    let j_ldf = JLazyFrame::new(ldf, global_ref);

    Box::into_raw(Box::new(j_ldf)) as jlong
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
    let this_path: String = env
        .get_string(filePath)
        .expect("Unable to get/ convert raw path to UTF8.")
        .into();

    let n_rows = if nRows.is_positive() {
        nRows.to_usize()
    } else {
        None
    };

    let row_count = if !rowCountColName.is_null() {
        Some(RowCount {
            name: env
                .get_string(rowCountColName)
                .expect("Unable to get/ convert row column name to UTF8.")
                .into(),
            offset: if rowCountColOffset.is_positive() {
                rowCountColOffset as IdxSize
            } else {
                0
            },
        })
    } else {
        None
    };

    let j_ldf = LazyCsvReader::new(this_path)
        .with_n_rows(n_rows)
        .with_delimiter(delimiter as u8)
        .has_header(hasHeader == JNI_TRUE)
        .with_ignore_parser_errors(ignoreErrors == JNI_TRUE)
        .with_row_count(row_count)
        .with_infer_schema_length(Some(inferSchemaRows as usize))
        .with_parse_dates(parseDates == JNI_TRUE);

    let ldf = j_ldf
        .finish()
        .expect("Cannot create LazyFrame from provided arguments.");

    let global_ref = env.new_global_ref(object).unwrap();
    let j_ldf = JLazyFrame::new(ldf, global_ref);

    Box::into_raw(Box::new(j_ldf)) as jlong
}
