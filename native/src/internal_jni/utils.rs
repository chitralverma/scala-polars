use std::path::{Component, PathBuf};

use jni::objects::{JObject, JString};
use jni::strings::JNIString;
use jni::sys::{jint, jlong, jstring};
use jni::JNIEnv;
use polars::io::RowIndex;
use polars::prelude::*;

use crate::j_data_frame::JDataFrame;
use crate::j_expr::JExpr;
use crate::j_lazy_frame::JLazyFrame;
use crate::j_series::JSeries;

pub fn normalize_path(path: &std::path::Path) -> PathBuf {
    let mut components = path.components().peekable();
    let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().cloned() {
        components.next();
        PathBuf::from(c.as_os_str())
    } else {
        PathBuf::new()
    };

    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            },
            Component::CurDir => {},
            Component::ParentDir => {
                ret.pop();
            },
            Component::Normal(c) => {
                ret.push(c);
            },
        }
    }

    ret
}

pub fn get_string(env: &mut JNIEnv, string: JString, error_msg: &str) -> String {
    env.get_string(&string).expect(error_msg).into()
}

pub fn to_jstring<S: Into<JNIString>>(env: &mut JNIEnv, string: S, error_msg: &str) -> jstring {
    env.new_string(string).expect(error_msg).into_raw()
}

pub fn get_file_path(env: &mut JNIEnv, file_path: JString) -> String {
    get_string(env, file_path, "Unable to get/ convert raw path to UTF8.")
}

pub fn get_n_rows(n_rows: jlong) -> Option<usize> {
    if n_rows.is_positive() {
        Some(n_rows as usize)
    } else {
        None
    }
}

pub fn get_row_index(
    env: &mut JNIEnv,
    row_count_col_name: JString,
    row_count_col_offset: jint,
) -> Option<RowIndex> {
    if !row_count_col_name.is_null() {
        Some(RowIndex {
            name: Arc::from(get_string(
                env,
                row_count_col_name,
                "Unable to get/ convert row column name to UTF8.",
            )),
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

pub fn ldf_to_ptr(env: &mut JNIEnv, object: JObject, ldf_res: PolarsResult<LazyFrame>) -> jlong {
    let ldf = ldf_res.expect("Cannot create LazyFrame from provided arguments.");

    let global_ref = env.new_global_ref(object).unwrap();
    let j_ldf = JLazyFrame::new(ldf, global_ref);

    Box::into_raw(Box::new(j_ldf)) as jlong
}

pub fn df_to_ptr(env: &mut JNIEnv, object: JObject, df_res: PolarsResult<DataFrame>) -> jlong {
    let df = df_res.expect("Cannot create LazyFrame from provided arguments.");

    let global_ref = env.new_global_ref(object).unwrap();
    let j_ldf = JDataFrame::new(df, global_ref);

    Box::into_raw(Box::new(j_ldf)) as jlong
}

pub fn expr_to_ptr(env: &mut JNIEnv, object: JObject, expr: Expr) -> jlong {
    let global_ref = env.new_global_ref(object).unwrap();
    let j_expr = JExpr::new(expr, global_ref);

    Box::into_raw(Box::new(j_expr)) as jlong
}

pub fn series_to_ptr(env: &mut JNIEnv, object: JObject, series_res: PolarsResult<Series>) -> jlong {
    let series = series_res.expect("Cannot create Series from provided arguments.");

    let global_ref = env.new_global_ref(object).unwrap();
    let j_series = JSeries::new(series, global_ref);

    Box::into_raw(Box::new(j_series)) as jlong
}
