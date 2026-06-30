use anyhow::Context;
use jni::objects::{JObject, JObjectArray, JString};
use jni::{Env, NativeMethod, native_method};
use polars::io::RowIndex;
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

const SCAN_CSV_METHOD: NativeMethod = scan_method!(
    extern fn scan_csv(paths: [java.lang.String], options: java.lang.String) -> LazyFrameHandle,
    name = "scanCSV",
);

fn scan_csv<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    paths: JObjectArray<'local, JString<'local>>,
    options: JString<'local>,
) -> anyhow::Result<LazyFrameHandle> {
    let mut options = parse_json_to_options(env, &options)?;

    let n_rows = opt_parse::<usize>(&mut options, "scan_csv_n_rows");

    let row_index_offset = options
        .remove("scan_csv_row_index_offset")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);

    let row_index = options
        .remove("scan_csv_row_index_name")
        .map(|name| RowIndex {
            name: name.into(),
            offset: row_index_offset,
        });

    let cache = options
        .remove("scan_csv_cache")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(true);

    let glob = options
        .remove("scan_csv_glob")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(true);

    let low_memory = options
        .remove("scan_csv_low_memory")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let rechunk = options
        .remove("scan_csv_rechunk")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let file_path_col = options
        .remove("scan_csv_include_file_paths")
        .map(PlSmallStr::from_string);

    let raise_if_empty = options
        .remove("scan_csv_raise_if_empty")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(true);

    let ignore_errors = options
        .remove("scan_csv_ignore_errors")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let has_header = options
        .remove("scan_csv_has_header")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(true);

    let missing_is_null = options
        .remove("scan_csv_missing_is_null")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(true);

    let truncate_ragged_lines = options
        .remove("scan_csv_truncate_ragged_lines")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let try_parse_dates = options
        .remove("scan_csv_try_parse_dates")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let decimal_comma = options
        .remove("scan_csv_decimal_comma")
        .and_then(|s| s.parse::<bool>().ok())
        .unwrap_or(false);

    let chunk_size = options
        .remove("scan_csv_chunk_size")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1 << 18);

    let skip_rows = options
        .remove("scan_csv_skip_rows")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    let skip_rows_after_header = options
        .remove("scan_csv_skip_rows_after_header")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    let infer_schema_length = options
        .remove("scan_csv_skip_infer_schema_length")
        .and_then(|s| s.parse::<usize>().ok())
        .map_or(Some(100), Some);

    let separator = options
        .remove("scan_csv_separator")
        .and_then(|s| s.parse::<u8>().ok())
        .unwrap_or(b',');

    let eol_char = options
        .remove("scan_csv_eol_char")
        .and_then(|s| s.parse::<u8>().ok())
        .unwrap_or(b'\n');

    let quote_char = options
        .remove("scan_csv_quote_char")
        .and_then(|s| s.parse::<u8>().ok())
        .map_or(Some(b'"'), Some);

    let encoding = options
        .remove("scan_csv_encoding")
        .map(|s| match s.as_str() {
            "lossy_utf8" => CsvEncoding::LossyUtf8,
            _ => CsvEncoding::Utf8,
        })
        .unwrap_or_default();

    let null_value = options
        .remove("scan_csv_null_value")
        .map(|s| NullValues::AllColumnsSingle(s.as_str().into()));

    let comment_prefix = options
        .remove("scan_csv_comment_prefix")
        .map(PlSmallStr::from);

    let (sources, cloud_options) = build_scan_sources(env, &paths, options)?;

    let ldf = LazyCsvReader::new_with_sources(sources)
        .with_glob(glob)
        .with_cache(cache)
        .with_include_file_paths(file_path_col)
        .with_low_memory(low_memory)
        .with_rechunk(rechunk)
        .with_n_rows(n_rows)
        .with_row_index(row_index)
        .with_raise_if_empty(raise_if_empty)
        .with_ignore_errors(ignore_errors)
        .with_has_header(has_header)
        .with_missing_is_null(missing_is_null)
        .with_truncate_ragged_lines(truncate_ragged_lines)
        .with_try_parse_dates(try_parse_dates)
        .with_decimal_comma(decimal_comma)
        .with_chunk_size(chunk_size)
        .with_skip_rows(skip_rows)
        .with_skip_rows_after_header(skip_rows_after_header)
        .with_infer_schema_length(infer_schema_length)
        .with_separator(separator)
        .with_quote_char(quote_char)
        .with_eol_char(eol_char)
        .with_encoding(encoding)
        .with_null_values(null_value)
        .with_comment_prefix(comment_prefix)
        .with_cloud_options(cloud_options)
        .finish()
        .context("Failed to perform csv scan")?;

    Ok(LazyFrameHandle::alloc(ldf))
}

pub const METHODS: &[NativeMethod] = &[SCAN_CSV_METHOD];
