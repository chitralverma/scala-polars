use jni::objects::{JObject, JString};
use jni::{Env, NativeMethod, native_method};
use polars::prelude::*;

use crate::internal_jni::handle::{DataFrameHandle, Handle};
use crate::internal_jni::io::parse_json_to_options;
use crate::internal_jni::io::write::{parse_overwrite_mode, write_dataframe};
use crate::utils::error::ThrowRuntimeException;

/// Wraps [`native_method!`] with the `io.write$` config common to every entry point in this module.
macro_rules! write_method {
    ($($tt:tt)*) => {
        native_method! {
            java_type = "com.github.chitralverma.polars.internal.jni.io.write$",
            error_policy = ThrowRuntimeException,
            type_map = { unsafe DataFrameHandle => long },
            $($tt)*
        }
    };
}

const WRITE_JSON_METHOD: NativeMethod = write_method!(
    extern fn write_json(df: DataFrameHandle, file_path: java.lang.String, options: java.lang.String),
    name = "writeJson",
);

fn write_json<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    df: DataFrameHandle,
    file_path: JString<'local>,
    options: JString<'local>,
) -> anyhow::Result<()> {
    let mut options = parse_json_to_options(env, &options)?;

    let json_format = options
        .remove("write_json_format")
        .and_then(|s| match s.to_lowercase().as_str() {
            "json" => Some(JsonFormat::Json),
            "json_lines" => Some(JsonFormat::JsonLines),
            _ => None,
        })
        .unwrap_or(JsonFormat::Json);

    let overwrite_mode = parse_overwrite_mode(&mut options);

    write_dataframe(
        env,
        df.get(),
        &file_path,
        overwrite_mode,
        options,
        "JSON",
        |writer, dataframe| {
            JsonWriter::new(writer)
                .with_json_format(json_format)
                .finish(dataframe)
        },
    )?;

    Ok(())
}

/// All native methods exported by this module.
pub const METHODS: &[NativeMethod] = &[WRITE_JSON_METHOD];
