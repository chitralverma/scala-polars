use jni::objects::{JObject, JString};
use jni::{Env, NativeMethod, native_method};
use num_traits::ToPrimitive;
use polars::prelude::*;
use polars_utils::compression::{BrotliLevel, GzipLevel, ZstdLevel};

use crate::internal_jni::handle::{DataFrameHandle, Handle};
use crate::internal_jni::io::write::{parse_overwrite_mode, write_dataframe};
use crate::internal_jni::io::{opt_parse, parse_json_to_options};
use crate::utils::error::ThrowRuntimeException;

/// Injects the shared `io.write$` config into [`native_method!`].
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

fn parse_parquet_compression(
    compression: Option<String>,
    compression_level: Option<i32>,
) -> Option<ParquetCompression> {
    match (compression, compression_level) {
        (Some(t), l) => match t.to_lowercase().as_str() {
            "uncompressed" => Some(ParquetCompression::Uncompressed),
            "snappy" => Some(ParquetCompression::Snappy),
            "gzip" => {
                let level = l.and_then(|v| GzipLevel::try_new(v.to_u8()?).ok());
                Some(ParquetCompression::Gzip(level))
            },
            "brotli" => {
                let level = l.and_then(|v| BrotliLevel::try_new(v.to_u32()?).ok());
                Some(ParquetCompression::Brotli(level))
            },
            "zstd" => {
                let level = l.and_then(|v| ZstdLevel::try_new(v).ok());
                Some(ParquetCompression::Zstd(level))
            },
            "lz4" => Some(ParquetCompression::Lz4Raw),
            e => {
                polars_warn!(
                    "Compression must be one of {{'uncompressed', 'snappy', 'gzip', 'brotli', 'lz4', 'zstd'}}, got {e}. Using defaults."
                );
                None
            },
        },
        _ => None,
    }
}

const WRITE_PARQUET_METHOD: NativeMethod = write_method!(
    extern fn write_parquet(df: DataFrameHandle, file_path: java.lang.String, options: java.lang.String),
    name = "writeParquet",
);

fn write_parquet<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    df: DataFrameHandle,
    file_path: JString<'local>,
    options: JString<'local>,
) -> anyhow::Result<()> {
    let mut options = parse_json_to_options(env, &options)?;

    let is_parallel = opt_parse::<bool>(&mut options, "write_parquet_parallel");

    let data_page_size = opt_parse::<usize>(&mut options, "write_parquet_data_page_size");

    let row_group_size = opt_parse::<usize>(&mut options, "write_parquet_row_group_size");

    let overwrite_mode = parse_overwrite_mode(&mut options);

    let compression = options.remove("write_compression");
    let compression_level = opt_parse::<i32>(&mut options, "write_compression_level");

    let write_stats = options
        .remove("write_parquet_stats")
        .map(|s| match s.as_str() {
            "full" => StatisticsOptions::full(),
            "none" => StatisticsOptions::empty(),
            _ => StatisticsOptions::default(),
        });

    write_dataframe(
        env,
        df.get(),
        &file_path,
        overwrite_mode,
        options,
        "Parquet",
        |writer, dataframe| {
            let parquet_compression = parse_parquet_compression(compression, compression_level);

            let mut parquet_writer = ParquetWriter::new(writer)
                .with_data_page_size(data_page_size)
                .with_row_group_size(row_group_size);

            if let Some(value) = is_parallel {
                parquet_writer = parquet_writer.set_parallel(value)
            }

            if let Some(value) = write_stats {
                parquet_writer = parquet_writer.with_statistics(value)
            }

            if let Some(value) = parquet_compression {
                parquet_writer = parquet_writer.with_compression(value)
            }

            parquet_writer.finish(dataframe).map(|_| ())
        },
    )?;

    Ok(())
}

pub const METHODS: &[NativeMethod] = &[WRITE_PARQUET_METHOD];
