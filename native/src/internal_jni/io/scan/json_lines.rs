use std::num::NonZeroUsize;
use std::str::FromStr;

use anyhow::Context;
use jni::objects::{JObject, JObjectArray, JString};
use jni::{Env, NativeMethod, native_method};
use polars::io::RowIndex;
use polars::prelude::*;

use crate::internal_jni::handle::{Handle, LazyFrameHandle};
use crate::internal_jni::io::scan::build_scan_sources;
use crate::internal_jni::io::{opt_parse, parse_json_to_options};
use crate::utils::error::ThrowRuntimeException;

const SCAN_JSON_LINES_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.io.scan$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe LazyFrameHandle => long },
    extern fn scan_json_lines(paths: [java.lang.String], options: java.lang.String) -> LazyFrameHandle,
    name = "scanJsonLines",
};

fn scan_json_lines<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    paths: JObjectArray<'local, JString<'local>>,
    options: JString<'local>,
) -> anyhow::Result<LazyFrameHandle> {
    let mut options = parse_json_to_options(env, &options)?;

    let n_rows = opt_parse::<usize>(&mut options, "scan_ndjson_n_rows");

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

    let (sources, cloud_options) = build_scan_sources(env, &paths, options)?;

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
        .context("Failed to perform ndjson scan")?;

    Ok(LazyFrameHandle::alloc(ldf))
}

/// All native methods exported by this module.
pub const METHODS: &[NativeMethod] = &[SCAN_JSON_LINES_METHOD];
