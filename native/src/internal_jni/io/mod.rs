use jni::objects::JString;
use jni::sys::jint;
use jni::JNIEnv;
use polars::io::RowIndex;
use polars::prelude::IdxSize;

use super::utils::j_string_to_string;

pub mod scan;
pub mod write;

pub fn get_file_path(env: &mut JNIEnv, file_path: JString) -> String {
    j_string_to_string(env, &file_path, Some("Failed to get provided path"))
}
pub fn get_row_index(
    env: &mut JNIEnv,
    row_count_col_name: JString,
    row_count_col_offset: jint,
) -> Option<RowIndex> {
    if !row_count_col_name.is_null() {
        Some(RowIndex {
            name: j_string_to_string(
                env,
                &row_count_col_name,
                Some("Failed to get the provided row column name"),
            )
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
