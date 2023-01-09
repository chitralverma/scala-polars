use crate::j_data_frame::JDataFrame;
use jni::objects::{JObject, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use polars::io::RowCount;
use polars::prelude::*;

use crate::j_lazy_frame::JLazyFrame;

pub fn get_file_path(env: JNIEnv, file_path: JString) -> String {
    env.get_string(file_path)
        .expect("Unable to get/ convert raw path to UTF8.")
        .into()
}

pub fn get_n_rows(n_rows: jlong) -> Option<usize> {
    if n_rows.is_positive() {
        Some(n_rows as usize)
    } else {
        None
    }
}

pub fn get_row_count(
    env: JNIEnv,
    row_count_col_name: JString,
    row_count_col_offset: jint,
) -> Option<RowCount> {
    if !row_count_col_name.is_null() {
        Some(RowCount {
            name: env
                .get_string(row_count_col_name)
                .expect("Unable to get/ convert row column name to UTF8.")
                .into(),
            offset: if row_count_col_offset.is_positive() {
                row_count_col_offset as IdxSize
            } else {
                0
            },
        })
    } else {
        None
    }
}

pub fn ldf_to_ptr(env: JNIEnv, object: JObject, ldf: PolarsResult<LazyFrame>) -> jlong {
    let ldf = ldf.expect("Cannot create LazyFrame from provided arguments.");

    let global_ref = env.new_global_ref(object).unwrap();
    let j_ldf = JLazyFrame::new(ldf, global_ref);

    Box::into_raw(Box::new(j_ldf)) as jlong
}

pub fn df_to_ptr(env: JNIEnv, object: JObject, ldf: PolarsResult<DataFrame>) -> jlong {
    let df = ldf.expect("Cannot create LazyFrame from provided arguments.");

    let global_ref = env.new_global_ref(object).unwrap();
    let j_ldf = JDataFrame::new(df, global_ref);

    Box::into_raw(Box::new(j_ldf)) as jlong
}
