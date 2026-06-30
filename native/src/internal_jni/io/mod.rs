use anyhow::Context;
use jni::Env;
use jni::objects::JString;
use jni::sys::jint;
use polars::io::RowIndex;
use polars::io::cloud::CloudOptions;
use polars::prelude::{CloudScheme, IdxSize, PlHashMap};

use super::utils::j_string_to_string;

pub mod scan;
pub mod write;

/// Removes `key` from `options` and parses its value into `T`, yielding `None` when the key is
/// absent or the value fails to parse. A faithful equivalent of the
/// `options.remove(key).and_then(|s| s.parse::<T>().ok())` idiom used throughout the IO modules.
pub(crate) fn opt_parse<T>(options: &mut PlHashMap<String, String>, key: &str) -> Option<T>
where
    T: std::str::FromStr,
{
    options.remove(key).and_then(|s| s.parse::<T>().ok())
}

pub fn get_file_path(env: &mut Env, file_path: &JString) -> anyhow::Result<String> {
    j_string_to_string(env, file_path, Some("Failed to get provided path"))
}

/// Parses untyped cloud options, propagating a parse failure as an error instead of
/// silently discarding it. For local/file URIs polars returns the default options, so a
/// failure only ever signals a genuine misconfiguration.
pub fn parse_cloud_options<I>(
    scheme: Option<CloudScheme>,
    options: I,
) -> anyhow::Result<Option<CloudOptions>>
where
    I: IntoIterator<Item = (String, String)>,
{
    CloudOptions::from_untyped_config(scheme, options)
        .map(Some)
        .context("Failed to parse the provided cloud options")
}

fn parse_json_to_options(
    env: &mut Env,
    options: &JString,
) -> anyhow::Result<PlHashMap<String, String>> {
    let s = j_string_to_string(
        env,
        options,
        Some("Failed to deserialize the provided options"),
    )?;
    serde_json::from_str(&s).context("Failed to parse the provided options")
}

pub fn get_row_index(
    env: &mut Env,
    row_count_col_name: &JString,
    row_count_col_offset: jint,
) -> anyhow::Result<Option<RowIndex>> {
    if !row_count_col_name.is_null() {
        Ok(Some(RowIndex {
            name: j_string_to_string(
                env,
                row_count_col_name,
                Some("Failed to get the provided row column name"),
            )?
            .into(),
            offset: if row_count_col_offset.is_positive() {
                row_count_col_offset as IdxSize
            } else {
                0
            },
        }))
    } else {
        Ok(None)
    }
}
