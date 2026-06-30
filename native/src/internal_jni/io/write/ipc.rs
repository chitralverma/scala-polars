use jni::objects::{JObject, JString};
use jni::{Env, NativeMethod, native_method};
use polars::prelude::*;
use polars_utils::compression::ZstdLevel;

use crate::internal_jni::handle::{DataFrameHandle, Handle};
use crate::internal_jni::io::write::{parse_overwrite_mode, write_dataframe};
use crate::internal_jni::io::{opt_parse, parse_json_to_options};
use crate::utils::error::ThrowRuntimeException;

/// Wraps [`native_method!`] with the `io.write$` config common to every entry point in this module.
macro_rules! write_method {
    ($($tt:tt)*) => {
        native_method! {
            java_type = "com.github.chitralverma.polars.internal.jni.io.write$",
            error_policy = ThrowRuntimeException,
            type_map = { unsafe DataFrameHandle => long },
            $($tt)*
        }
    };
}

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

const WRITE_IPC_METHOD: NativeMethod = write_method!(
    extern fn write_ipc(df: DataFrameHandle, file_path: java.lang.String, options: java.lang.String),
    name = "writeIPC",
);

fn write_ipc<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    df: DataFrameHandle,
    file_path: JString<'local>,
    options: JString<'local>,
) -> anyhow::Result<()> {
    let mut options = parse_json_to_options(env, &options)?;

    let compat_level =
        options
            .remove("write_ipc_compat_level")
            .map(|s| match s.to_lowercase().as_str() {
                "newest" => CompatLevel::newest(),
                _ => CompatLevel::oldest(),
            });

    let overwrite_mode = parse_overwrite_mode(&mut options);

    let compression = options.remove("write_compression");
    let compression_level = opt_parse::<i32>(&mut options, "write_compression_level");

    write_dataframe(
        env,
        df.get(),
        &file_path,
        overwrite_mode,
        options,
        "IPC",
        |writer, dataframe| {
            let ipc_compression = parse_ipc_compression(compression, compression_level);

            let mut ipc_writer = IpcWriter::new(writer).with_compression(ipc_compression);

            if let Some(value) = compat_level {
                ipc_writer = ipc_writer.with_compat_level(value)
            }

            ipc_writer.finish(dataframe)
        },
    )?;

    Ok(())
}

/// All native methods exported by this module.
pub const METHODS: &[NativeMethod] = &[WRITE_IPC_METHOD];
