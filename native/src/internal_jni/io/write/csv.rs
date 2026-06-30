use jni::objects::{JObject, JString};
use jni::{Env, NativeMethod, native_method};
use polars::prelude::*;

use crate::internal_jni::handle::{DataFrameHandle, Handle};
use crate::internal_jni::io::write::{parse_overwrite_mode, write_dataframe};
use crate::internal_jni::io::{opt_parse, parse_json_to_options};
use crate::utils::error::ThrowRuntimeException;

const WRITE_CSV_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.io.write$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe DataFrameHandle => long },
    extern fn write_csv(df: DataFrameHandle, file_path: java.lang.String, options: java.lang.String),
    name = "writeCSV",
};

fn write_csv<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    df: DataFrameHandle,
    file_path: JString<'local>,
    options: JString<'local>,
) -> anyhow::Result<()> {
    let mut options = parse_json_to_options(env, &options)?;

    let include_bom = opt_parse::<bool>(&mut options, "write_csv_include_bom");

    let include_header = opt_parse::<bool>(&mut options, "write_csv_include_header");

    let float_scientific = opt_parse::<bool>(&mut options, "write_csv_float_scientific");

    let float_precision = opt_parse::<usize>(&mut options, "write_csv_float_precision");

    let separator = opt_parse::<u8>(&mut options, "write_csv_separator");

    let quote_char = opt_parse::<u8>(&mut options, "write_csv_quote_char");

    let date_format = options.remove("write_csv_date_format").map(|s| s.into());
    let time_format = options.remove("write_csv_time_format").map(|s| s.into());
    let datetime_format = options
        .remove("write_csv_datetime_format")
        .map(|s| s.into());

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

    let overwrite_mode = parse_overwrite_mode(&mut options);

    write_dataframe(
        env,
        df.get(),
        &file_path,
        overwrite_mode,
        options,
        "CSV",
        |writer, dataframe| {
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
                csv_writer = csv_writer.with_line_terminator(value.into())
            }

            if let Some(value) = null_value {
                csv_writer = csv_writer.with_null_value(value.into())
            }

            if let Some(value) = quote_style {
                csv_writer = csv_writer.with_quote_style(value)
            }

            csv_writer.finish(dataframe)
        },
    )?;

    Ok(())
}

/// All native methods exported by this module.
pub const METHODS: &[NativeMethod] = &[WRITE_CSV_METHOD];
