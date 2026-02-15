#![allow(non_snake_case)]

use anyhow::Context;
use jni::JNIEnv;
use jni::objects::{JObject, JString};
use jni_fn::jni_fn;
use polars::prelude::*;
use polars_utils::compression::ZstdLevel;

use crate::internal_jni::io::parse_json_to_options;
use crate::internal_jni::io::write::get_df_and_writer;
use crate::utils::error::ResultExt;

fn parse_ipc_compression(
    compression: Option<String>,
    compression_level: Option<i32>,
) -> Option<IpcCompression> {
    match (compression, compression_level) {
        (Some(t), l) => match t.to_lowercase().as_str() {
            "uncompressed" => None,
            "lz4" => Some(IpcCompression::LZ4),
            "zstd" => {
                let level = l
                    .and_then(|v| ZstdLevel::try_new(v).ok())
                    .unwrap_or_default();
                Some(IpcCompression::ZSTD(level))
            },
            e => {
                polars_warn!(
                    "Compression must be one of {{'uncompressed', 'lz4', 'zstd'}}, got {e}. Using defaults."
                );
                None
            },
        },
        _ => None,
    }
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.io.write$")]
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
    let compression_level = options
        .remove("write_compression_level")
        .and_then(|s| s.parse::<i32>().ok());

    let (mut dataframe, writer) =
        get_df_and_writer(&mut env, df_ptr, filePath, overwrite_mode, options);

    let ipc_compression = parse_ipc_compression(compression, compression_level);

    let mut ipc_writer = IpcWriter::new(writer).with_compression(ipc_compression);

    if let Some(value) = compat_level {
        ipc_writer = ipc_writer.with_compat_level(value)
    }

    ipc_writer
        .finish(&mut dataframe)
        .context("Failed to write IPC data")
        .unwrap_or_throw(&mut env);
}
