use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use jni::objects::{JClass, JObject, JObjectArray, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::io::cloud::CloudOptions;
use polars::io::{HiveOptions, RowIndex};
use polars::prelude::*;

use crate::internal_jni::io::{get_file_path, parse_json_to_options};
use crate::internal_jni::utils::{to_ptr, JavaArrayToVec};
use crate::utils::error::ResultExt;

#[jni_fn("org.polars.scala.polars.internal.jni.io.scan$")]
pub unsafe fn scanIPC(mut env: JNIEnv, _: JClass, paths: JObjectArray, options: JString) -> jlong {
    let mut options = parse_json_to_options(&mut env, options);

    let n_rows = options
        .remove("scan_ipc_n_rows")
        .and_then(|s| s.parse::<usize>().ok());

    let cache = options
        .remove("scan_ipc_cache")
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

    let paths_vec: Vec<PathBuf> = JavaArrayToVec::to_vec(&mut env, paths)
        .into_iter()
        .map(|o| JObject::from_raw(o))
        .map(|o| get_file_path(&mut env, JString::from(o)))
        .map(PathBuf::from)
        .collect();

    let first_path = paths_vec
        .first()
        .and_then(|p| p.to_str())
        .context("Failed to get first path from provided list of paths")
        .unwrap_or_throw(&mut env);

    let cloud_options = CloudOptions::from_untyped_config(first_path, &options).ok();

    let scan_args = ScanArgsIpc {
        n_rows,
        cache,
        rechunk,
        row_index,
        cloud_options,
        hive_options: HiveOptions {
            enabled: hive_scan_partitions,
            hive_start_idx: 0,
            schema: None,
            try_parse_dates: hive_try_parse_dates,
        },
        include_file_paths: file_path_col,
    };

    let ldf = LazyFrame::scan_ipc_files(Arc::from(paths_vec.into_boxed_slice()), scan_args)
        .context("Failed to perform ipc scan")
        .unwrap_or_throw(&mut env);

    to_ptr(ldf)
}
