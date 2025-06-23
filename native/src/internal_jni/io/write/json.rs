#![allow(non_snake_case)]

use anyhow::Context;
use jni::objects::{JObject, JString};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::prelude::*;

use crate::internal_jni::io::parse_json_to_options;
use crate::internal_jni::io::write::get_df_and_writer;
use crate::utils::error::ResultExt;

#[jni_fn("com.github.chitralverma.polars.internal.jni.io.write$")]
pub fn writeJson(
    mut env: JNIEnv,
    _object: JObject,
    df_ptr: *mut DataFrame,
    filePath: JString,
    options: JString,
) {
    let mut options = parse_json_to_options(&mut env, options);

    let json_format = options
        .remove("write_json_format")
        .and_then(|s| match s.to_lowercase().as_str() {
            "json" => Some(JsonFormat::Json),
            "json_lines" => Some(JsonFormat::JsonLines),
            _ => None,
        })
        .unwrap_or(JsonFormat::Json);

    let overwrite_mode = options
        .remove("write_mode")
        .map(|s| matches!(s.to_lowercase().as_str(), "overwrite"))
        .unwrap_or(false);

    let (mut dataframe, writer) =
        get_df_and_writer(&mut env, df_ptr, filePath, overwrite_mode, options);

    let mut json_writer = JsonWriter::new(writer).with_json_format(json_format);

    json_writer
        .finish(&mut dataframe)
        .context("Failed to write JSON data")
        .unwrap_or_throw(&mut env);
}
