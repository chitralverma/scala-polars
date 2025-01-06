#![allow(non_snake_case)]

use anyhow::Context;
use jni::objects::{JObject, JString};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::prelude::*;

use crate::internal_jni::io::parse_json_to_options;
use crate::internal_jni::io::write::get_df_and_writer;
use crate::utils::error::ResultExt;

fn parse_ipc_compression(compression: Option<String>) -> Option<IpcCompression> {
    match compression {
        Some(t) => match t.to_lowercase().as_str() {
            "uncompressed" => None,
            "lz4" => Some(IpcCompression::LZ4),
            "zstd" => Some(IpcCompression::ZSTD),
            e => {
                polars_warn!(format!(
                    "Compression must be one of {{'uncompressed', 'lz4', 'zstd'}}, got {e}. Using defaults."
                ));
                None
            },
        },
        _ => None,
    }
}

#[jni_fn("org.polars.scala.polars.internal.jni.io.write$")]
pub fn writeIPC(
    mut env: JNIEnv,
    _object: JObject,
    df_ptr: *mut DataFrame,
    filePath: JString,
    options: JString,
) {
    let mut options = parse_json_to_options(&mut env, options);

    let compat_level =
        options
            .remove("write_ipc_compat_level")
            .map(|s| match s.to_lowercase().as_str() {
                "newest" => CompatLevel::newest(),
                _ => CompatLevel::oldest(),
            });

    let overwrite_mode = options
        .remove("write_mode")
        .map(|s| matches!(s.to_lowercase().as_str(), "overwrite"))
        .unwrap_or(false);

    let compression = options.remove("write_compression");

    let (mut dataframe, writer) =
        get_df_and_writer(&mut env, df_ptr, filePath, overwrite_mode, options);

    let ipc_compression = parse_ipc_compression(compression);

    let mut ipc_writer = IpcWriter::new(writer).with_compression(ipc_compression);

    if let Some(value) = compat_level {
        ipc_writer = ipc_writer.with_compat_level(value)
    }

    ipc_writer
        .finish(&mut dataframe)
        .context("Failed to write IPC data")
        .unwrap_or_throw(&mut env);
}
