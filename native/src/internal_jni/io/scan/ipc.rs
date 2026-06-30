use anyhow::Context;
use jni::objects::{JObject, JObjectArray, JString};
use jni::{Env, NativeMethod, native_method};
use polars::io::{HiveOptions, RowIndex};
use polars::prelude::*;

use crate::internal_jni::handle::{Handle, LazyFrameHandle};
use crate::internal_jni::io::parse_json_to_options;
use crate::internal_jni::io::scan::build_scan_sources;
use crate::utils::error::ThrowRuntimeException;

/// Wraps [`native_method!`] with the `io.scan$` config common to every entry point in this module.
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

const SCAN_IPC_METHOD: NativeMethod = scan_method!(
    extern fn scan_ipc(paths: [java.lang.String], options: java.lang.String) -> LazyFrameHandle,
    name = "scanIPC",
);

fn scan_ipc<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    paths: JObjectArray<'local, JString<'local>>,
    options: JString<'local>,
) -> anyhow::Result<LazyFrameHandle> {
    let mut options = parse_json_to_options(env, &options)?;

    let use_statistics = options
        .remove("scan_ipc_use_statistics")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(true);

    let cache = options
        .remove("scan_ipc_cache")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(true);

    let glob = options
        .remove("scan_ipc_glob")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(true);

    let rechunk = options
        .remove("scan_ipc_rechunk")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let row_index_offset = options
        .remove("scan_ipc_row_index_offset")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);

    let row_index = options
        .remove("scan_ipc_row_index_name")
        .map(|name| RowIndex {
            name: name.into(),
            offset: row_index_offset,
        });

    let file_path_col = options
        .remove("scan_ipc_include_file_paths")
        .map(PlSmallStr::from_string);

    let hive_scan_partitions = options
        .remove("scan_ipc_hive_scan_partitions")
        .and_then(|s| s.parse::<bool>().ok())
        .map_or(Some(true), Some);

    let hive_try_parse_dates = options
        .remove("scan_ipc_hive_try_parse_dates")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(true);

    let (sources, cloud_options) = build_scan_sources(env, &paths, options)?;

    let options = IpcScanOptions {
        record_batch_statistics: use_statistics,
        checked: Default::default(),
    };

    let unified_scan_args = UnifiedScanArgs {
        cache,
        rechunk,
        row_index,
        glob,
        cloud_options,
        hive_options: HiveOptions {
            enabled: hive_scan_partitions,
            hive_start_idx: 0,
            schema: Default::default(),
            try_parse_dates: hive_try_parse_dates,
        },
        include_file_paths: file_path_col,
        ..Default::default()
    };

    let ldf = LazyFrame::scan_ipc_sources(sources, options, unified_scan_args)
        .context("Failed to perform ipc scan")?;

    Ok(LazyFrameHandle::alloc(ldf))
}

/// All native methods exported by this module.
pub const METHODS: &[NativeMethod] = &[SCAN_IPC_METHOD];
