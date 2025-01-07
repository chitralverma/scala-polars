#![allow(non_snake_case)]

use anyhow::Context;
use jni::objects::{JObject, JString};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::prelude::*;

use crate::internal_jni::io::parse_json_to_options;
use crate::internal_jni::io::write::get_df_and_writer;
use crate::utils::error::ResultExt;

#[jni_fn("org.polars.scala.polars.internal.jni.io.write$")]
pub fn writeCSV(
    mut env: JNIEnv,
    _object: JObject,
    df_ptr: *mut DataFrame,
    filePath: JString,
    options: JString,
) {
    let mut options = parse_json_to_options(&mut env, options);

    let include_bom = options
        .remove("write_csv_include_bom")
        .and_then(|s| s.parse::<bool>().ok());

    let include_header = options
        .remove("write_csv_include_header")
        .and_then(|s| s.parse::<bool>().ok());

    let float_scientific = options
        .remove("write_csv_float_scientific")
        .and_then(|s| s.parse::<bool>().ok());

    let float_precision = options
        .remove("write_csv_float_precision")
        .and_then(|s| s.parse::<usize>().ok());

    let separator = options
        .remove("write_csv_separator")
        .and_then(|s| s.parse::<u8>().ok());

    let quote_char = options
        .remove("write_csv_quote_char")
        .and_then(|s| s.parse::<u8>().ok());

    let date_format = options.remove("write_csv_date_format");
    let time_format = options.remove("write_csv_time_format");
    let datetime_format = options.remove("write_csv_datetime_format");

    let line_terminator = options.remove("write_csv_line_terminator");
    let null_value = options.remove("write_csv_null_value");

    let quote_style = options
        .remove("write_csv_quote_style")
        .map(|s| match s.as_str() {
            "always" => QuoteStyle::Always,
            "non_numeric" => QuoteStyle::NonNumeric,
            "never" => QuoteStyle::Never,
            _ => QuoteStyle::Necessary,
        });

    let overwrite_mode = options
        .remove("write_mode")
        .map(|s| matches!(s.to_lowercase().as_str(), "overwrite"))
        .unwrap_or(false);

    let (mut dataframe, writer) =
        get_df_and_writer(&mut env, df_ptr, filePath, overwrite_mode, options);

    let mut csv_writer = CsvWriter::new(writer)
        .with_date_format(date_format)
        .with_time_format(time_format)
        .with_datetime_format(datetime_format)
        .with_float_precision(float_precision)
        .with_float_scientific(float_scientific);

    if let Some(value) = include_bom {
        csv_writer = csv_writer.include_bom(value)
    }

    if let Some(value) = include_header {
        csv_writer = csv_writer.include_header(value)
    }

    if let Some(value) = separator {
        csv_writer = csv_writer.with_separator(value)
    }

    if let Some(value) = quote_char {
        csv_writer = csv_writer.with_quote_char(value)
    }

    if let Some(value) = line_terminator {
        csv_writer = csv_writer.with_line_terminator(value)
    }

    if let Some(value) = null_value {
        csv_writer = csv_writer.with_null_value(value)
    }

    if let Some(value) = quote_style {
        csv_writer = csv_writer.with_quote_style(value)
    }

    csv_writer
        .finish(&mut dataframe)
        .context("Failed to write CSV data")
        .unwrap_or_throw(&mut env);
}
