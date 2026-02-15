use anyhow::Context;
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JObjectArray, JString};
use jni::sys::jlong;
use jni_fn::jni_fn;
use polars::io::cloud::CloudOptions;
use polars::io::{HiveOptions, RowIndex};
use polars::prelude::*;

use crate::internal_jni::io::{get_file_path, parse_json_to_options};
use crate::internal_jni::utils::{JavaArrayToVec, to_ptr};
use crate::utils::error::ResultExt;

#[jni_fn("com.github.chitralverma.polars.internal.jni.io.scan$")]
pub unsafe fn scanIPC(mut env: JNIEnv, _: JClass, paths: JObjectArray, options: JString) -> jlong {
    let mut options = parse_json_to_options(&mut env, options);

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

    let paths_vec: Vec<PlRefPath> = JavaArrayToVec::to_vec(&mut env, paths)
        .into_iter()
        .map(|o| unsafe { JObject::from_raw(o) })
        .map(|o| get_file_path(&mut env, JString::from(o)))
        .map(PlRefPath::new)
        .collect();

    let sources = ScanSources::Paths(paths_vec.into());
    let cloud_scheme = sources
        .first_path()
        .cloned()
        .as_ref()
        .and_then(|x| x.scheme());

    let cloud_options = CloudOptions::from_untyped_config(cloud_scheme, options).ok();

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
        .context("Failed to perform ipc scan")
        .unwrap_or_throw(&mut env);

    to_ptr(ldf)
}
