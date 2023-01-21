use crate::j_data_frame::JDataFrame;
use crate::j_expr::JExpr;
use jni::objects::{JObject, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use polars::io::RowCount;
use polars::prelude::*;

use crate::j_lazy_frame::JLazyFrame;

pub fn get_string(env: JNIEnv, string: JString, error_msg: &str) -> String {
    env.get_string(string).expect(error_msg).into()
}

pub fn get_file_path(env: JNIEnv, file_path: JString) -> String {
    get_string(env, file_path, "Unable to get/ convert raw path to UTF8.")
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

pub fn ldf_to_ptr(env: JNIEnv, object: JObject, ldf_res: PolarsResult<LazyFrame>) -> jlong {
    let ldf = ldf_res.expect("Cannot create LazyFrame from provided arguments.");

    let global_ref = env.new_global_ref(object).unwrap();
    let j_ldf = JLazyFrame::new(ldf, global_ref);

    Box::into_raw(Box::new(j_ldf)) as jlong
}

pub fn df_to_ptr(env: JNIEnv, object: JObject, df_res: PolarsResult<DataFrame>) -> jlong {
    let df = df_res.expect("Cannot create LazyFrame from provided arguments.");

    let global_ref = env.new_global_ref(object).unwrap();
    let j_ldf = JDataFrame::new(df, global_ref);

    Box::into_raw(Box::new(j_ldf)) as jlong
}

pub fn expr_to_ptr(env: JNIEnv, object: JObject, expr: Expr) -> jlong {
    let global_ref = env.new_global_ref(object).unwrap();
    let j_expr = JExpr::new(expr, global_ref);

    Box::into_raw(Box::new(j_expr)) as jlong
}
