use std::num::NonZeroUsize;
use std::str::FromStr;

use anyhow::Context;
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JObjectArray, JString};
use jni::sys::jlong;
use jni_fn::jni_fn;
use polars::io::RowIndex;
use polars::io::cloud::CloudOptions;
use polars::prelude::*;

use crate::internal_jni::conversion::JavaArrayToVec;
use crate::internal_jni::io::{get_file_path, parse_json_to_options};
use crate::internal_jni::utils::to_ptr;
use crate::utils::error::ResultExt;

#[jni_fn("com.github.chitralverma.polars.internal.jni.io.scan$")]
pub unsafe fn scanJsonLines(
    mut env: JNIEnv,
    _: JClass,
    paths: JObjectArray,
    options: JString,
) -> jlong {
    let mut options = parse_json_to_options(&mut env, options);

    let n_rows = options
        .remove("scan_ndjson_n_rows")
        .and_then(|s| s.parse::<usize>().ok());

    let row_index_offset = options
        .remove("scan_ndjson_row_index_offset")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);

    let row_index = options
        .remove("scan_ndjson_row_index_name")
        .map(|name| RowIndex {
            name: name.into(),
            offset: row_index_offset,
        });

    let low_memory = options
        .remove("scan_ndjson_low_memory")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let rechunk = options
        .remove("scan_ndjson_rechunk")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let file_path_col = options
        .remove("scan_ndjson_include_file_paths")
        .map(PlSmallStr::from_string);

    let ignore_errors = options
        .remove("scan_ndjson_ignore_errors")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let batch_size = options
        .remove("scan_ndjson_batch_size")
        .and_then(|s| NonZeroUsize::from_str(s.as_str()).ok());

    let infer_schema_length = options
        .remove("scan_ndjson_infer_schema_length")
        .and_then(|s| NonZeroUsize::from_str(s.as_str()).ok())
        .map_or(NonZeroUsize::new(100), Some);

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

    let ldf = LazyJsonLineReader::new_with_sources(sources)
        .low_memory(low_memory)
        .with_rechunk(rechunk)
        .with_n_rows(n_rows)
        .with_row_index(row_index)
        .with_infer_schema_length(infer_schema_length)
        .with_ignore_errors(ignore_errors)
        .with_batch_size(batch_size)
        .with_include_file_paths(file_path_col)
        .with_cloud_options(cloud_options)
        .finish()
        .context("Failed to perform ndjson scan")
        .unwrap_or_throw(&mut env);

    to_ptr(ldf)
}
