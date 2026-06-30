use anyhow::Context;
use jni::JNIEnv;
use jni::objects::JString;
use jni::sys::jint;
use polars::io::RowIndex;
use polars::io::cloud::CloudOptions;
use polars::prelude::{CloudScheme, IdxSize, PlHashMap};

use super::utils::j_string_to_string;
use crate::utils::error::{ResultExt, throw_java_exception};

pub mod scan;
pub mod write;

pub fn get_file_path(env: &mut JNIEnv, file_path: JString) -> String {
    j_string_to_string(env, &file_path, Some("Failed to get provided path"))
}

/// Parses untyped cloud options, throwing a Java exception (and returning `None`) on a
/// parse failure instead of silently discarding the error. For local/file URIs polars
/// returns the default options, so `None` is only ever produced on a genuine error.
pub fn parse_cloud_options<I>(
    env: &mut JNIEnv,
    scheme: Option<CloudScheme>,
    options: I,
) -> Option<CloudOptions>
where
    I: IntoIterator<Item = (String, String)>,
{
    match CloudOptions::from_untyped_config(scheme, options) {
        Ok(opts) => Some(opts),
        Err(err) => {
            throw_java_exception(
                env,
                anyhow::Error::new(err).context("Failed to parse the provided cloud options"),
            );
            None
        },
    }
}

fn parse_json_to_options(env: &mut JNIEnv, options: JString) -> PlHashMap<String, String> {
    Ok(j_string_to_string(
        env,
        &options,
        Some("Failed to deserialize the provided options"),
    ))
    .and_then(|s| serde_json::from_str(&s))
    .context("Failed to parse the provided options")
    .unwrap_or_throw(env)
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
