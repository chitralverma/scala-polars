#![allow(non_snake_case)]

use jni::objects::{JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use jni_fn::jni_fn;
use num_traits::ToPrimitive;
use polars::prelude::*;

use crate::internal_jni::io::write::{get_df_and_writer, parse_json_to_options, DynWriter};

fn parse_parquet_compression(
    compression: Option<String>,
    compression_level: Option<i32>,
) -> Option<ParquetCompression> {
    match (compression, compression_level) {
        (Some(t), l) => match t.to_lowercase().as_str() {
            "uncompressed" => Some(ParquetCompression::Uncompressed),
            "snappy" => Some(ParquetCompression::Snappy),
            "lz4" => Some(ParquetCompression::Lz4Raw),
            "lzo" => Some(ParquetCompression::Lzo),
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
            e => {
                polars_warn!(format!("Compression must be one of {{'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'lz4', 'zstd'}}, got {e}. Using defaults."));
                None
            },
        },
        _ => None,
    }
}

#[jni_fn("org.polars.scala.polars.internal.jni.io.write$")]
pub fn writeParquet(
    mut env: JNIEnv,
    _object: JObject,
    df_ptr: jlong,
    filePath: JString,
    options: JString,
) {
    let mut options = parse_json_to_options(&mut env, options).unwrap();

    let is_parallel = options
        .remove("write_parquet_parallel")
        .and_then(|s| s.parse::<bool>().ok());

    let data_page_size = options
        .remove("write_parquet_data_page_size")
        .and_then(|s| s.parse::<usize>().ok());

    let row_group_size = options
        .remove("write_parquet_row_group_size")
        .and_then(|s| s.parse::<usize>().ok());

    let overwrite_mode = options
        .remove("write_mode")
        .map(|s| matches!(s.to_lowercase().as_str(), "overwrite"))
        .unwrap_or(false);

    let compression = options.remove("write_compression");
    let compression_level = options
        .remove("write_compression_level")
        .and_then(|s| s.parse::<i32>().ok());

    let write_stats = options
        .remove("write_parquet_stats")
        .map(|s| match s.as_str() {
            "full" => StatisticsOptions::full(),
            "none" => StatisticsOptions::empty(),
            _ => StatisticsOptions::default(),
        });

    let (mut dataframe, writer): (DataFrame, DynWriter) =
        get_df_and_writer(&mut env, df_ptr, filePath, overwrite_mode, options).unwrap();

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

    parquet_writer.finish(&mut dataframe).unwrap();
}
