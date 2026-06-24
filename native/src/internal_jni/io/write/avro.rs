#![allow(non_snake_case)]

use jni::JNIEnv;
use jni::objects::{JObject, JString};
use jni_fn::jni_fn;
use polars::io::avro::{AvroCompression, AvroWriter};
use polars::prelude::*;

use crate::internal_jni::io::parse_json_to_options;
use crate::internal_jni::io::write::write_dataframe;
use crate::internal_jni::utils::from_ptr;

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

#[jni_fn("com.github.chitralverma.polars.internal.jni.io.write$")]
pub fn writeAvro(
    mut env: JNIEnv,
    _object: JObject,
    df_ptr: *mut DataFrame,
    filePath: JString,
    options: JString,
) {
    let mut options = parse_json_to_options(&mut env, options);

    let record_name = options.remove("write_avro_record_name");

    let overwrite_mode = options
        .remove("write_mode")
        .map(|s| matches!(s.to_lowercase().as_str(), "overwrite"))
        .unwrap_or(false);

    let compression = options.remove("write_compression");

    write_dataframe(
        &mut env,
        from_ptr(df_ptr),
        filePath,
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
    );
}
