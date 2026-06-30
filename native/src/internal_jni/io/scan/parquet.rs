use anyhow::Context;
use jni::objects::{JObject, JObjectArray, JString};
use jni::{Env, NativeMethod, native_method};
use polars::io::{HiveOptions, RowIndex};
use polars::prelude::*;

use crate::internal_jni::handle::{Handle, LazyFrameHandle};
use crate::internal_jni::io::scan::build_scan_sources;
use crate::internal_jni::io::{opt_parse, parse_json_to_options};
use crate::utils::error::ThrowRuntimeException;

/// Injects the shared `io.scan$` config into [`native_method!`].
macro_rules! scan_method {
    ($($tt:tt)*) => {
        native_method! {
            java_type = "com.github.chitralverma.polars.internal.jni.io.scan$",
            error_policy = ThrowRuntimeException,
            type_map = { unsafe LazyFrameHandle => long },
            $($tt)*
        }
    };
}

const SCAN_PARQUET_METHOD: NativeMethod = scan_method!(
    extern fn scan_parquet(paths: [java.lang.String], options: java.lang.String) -> LazyFrameHandle,
    name = "scanParquet",
);

fn scan_parquet<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    paths: JObjectArray<'local, JString<'local>>,
    options: JString<'local>,
) -> anyhow::Result<LazyFrameHandle> {
    let mut options = parse_json_to_options(env, &options)?;

    let n_rows = opt_parse::<usize>(&mut options, "scan_parquet_n_rows");

    let parallel = options
        .remove("scan_parquet_parallel")
        .map(|s| match s.as_str() {
            "columns" => ParallelStrategy::Columns,
            "prefiltered" => ParallelStrategy::Prefiltered,
            "row_groups" => ParallelStrategy::RowGroups,
            "none" => ParallelStrategy::None,
            _ => ParallelStrategy::default(),
        })
        .unwrap_or_default();

    let row_index_offset = options
        .remove("scan_parquet_row_index_offset")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);

    let row_index = options
        .remove("scan_parquet_row_index_name")
        .map(|name| RowIndex {
            name: name.into(),
            offset: row_index_offset,
        });

    let use_statistics = options
        .remove("scan_parquet_use_statistics")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(true);

    let cache = options
        .remove("scan_parquet_cache")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(true);

    let glob = options
        .remove("scan_parquet_glob")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(true);

    let low_memory = options
        .remove("scan_parquet_low_memory")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let rechunk = options
        .remove("scan_parquet_rechunk")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let allow_missing_columns = options
        .remove("scan_parquet_allow_missing_columns")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let file_path_col = options
        .remove("scan_parquet_include_file_paths")
        .map(PlSmallStr::from_string);

    let hive_scan_partitions = options
        .remove("scan_parquet_hive_scan_partitions")
        .and_then(|s| s.parse::<bool>().ok())
        .map_or(Some(true), Some);

    let hive_try_parse_dates = options
        .remove("scan_parquet_hive_try_parse_dates")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(true);

    let (sources, cloud_options) = build_scan_sources(env, &paths, options)?;

    let scan_args = ScanArgsParquet {
        n_rows,
        parallel,
        row_index,
        use_statistics,
        cache,
        glob,
        low_memory,
        rechunk,
        allow_missing_columns,
        cloud_options,
        include_file_paths: file_path_col,
        hive_options: HiveOptions {
            enabled: hive_scan_partitions,
            hive_start_idx: 0,
            schema: None,
            try_parse_dates: hive_try_parse_dates,
        },
        schema: None,
    };

    let ldf = LazyFrame::scan_parquet_sources(sources, scan_args)
        .context("Failed to perform parquet scan")?;

    Ok(LazyFrameHandle::alloc(ldf))
}

pub const METHODS: &[NativeMethod] = &[SCAN_PARQUET_METHOD];
