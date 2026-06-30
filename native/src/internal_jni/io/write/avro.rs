use jni::objects::{JObject, JString};
use jni::{Env, NativeMethod, native_method};
use polars::io::avro::{AvroCompression, AvroWriter};
use polars::prelude::*;

use crate::internal_jni::handle::{DataFrameHandle, Handle};
use crate::internal_jni::io::parse_json_to_options;
use crate::internal_jni::io::write::{parse_overwrite_mode, write_dataframe};
use crate::utils::error::ThrowRuntimeException;

fn parse_avro_compression(compression: Option<String>) -> Option<AvroCompression> {
    match compression {
        Some(t) => match t.to_lowercase().as_str() {
            "uncompressed" => None,
            "deflate" => Some(AvroCompression::Deflate),
            "snappy" => Some(AvroCompression::Snappy),
            e => {
                polars_warn!(
                    "Compression must be one of {{'uncompressed', 'deflate', 'snappy'}}, got {e}. Using defaults."
                );
                None
            },
        },
        _ => None,
    }
}

const WRITE_AVRO_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.io.write$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe DataFrameHandle => long },
    extern fn write_avro(df: DataFrameHandle, file_path: java.lang.String, options: java.lang.String),
    name = "writeAvro",
};

fn write_avro<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    df: DataFrameHandle,
    file_path: JString<'local>,
    options: JString<'local>,
) -> anyhow::Result<()> {
    let mut options = parse_json_to_options(env, &options)?;

    let record_name = options.remove("write_avro_record_name");

    let overwrite_mode = parse_overwrite_mode(&mut options);

    let compression = options.remove("write_compression");

    write_dataframe(
        env,
        df.get(),
        &file_path,
        overwrite_mode,
        options,
        "Avro",
        |writer, dataframe| {
            let avro_compression = parse_avro_compression(compression);

            let mut avro_writer = AvroWriter::new(writer).with_compression(avro_compression);

            if let Some(value) = record_name {
                avro_writer = avro_writer.with_name(value)
            }

            avro_writer.finish(dataframe)
        },
    )?;

    Ok(())
}

/// All native methods exported by this module.
pub const METHODS: &[NativeMethod] = &[WRITE_AVRO_METHOD];
