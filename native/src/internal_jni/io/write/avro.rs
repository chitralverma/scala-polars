#![allow(non_snake_case)]

use jni::objects::{JObject, JString};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::io::avro::{AvroCompression, AvroWriter};
use polars::prelude::*;

use crate::internal_jni::io::write::{get_df_and_writer, parse_json_to_options, DynWriter};

fn parse_avro_compression(compression: Option<String>) -> Option<AvroCompression> {
    match compression {
        Some(t) => match t.to_lowercase().as_str() {
            "uncompressed" => None,
            "deflate" => Some(AvroCompression::Deflate),
            "snappy" => Some(AvroCompression::Snappy),
            e => {
                polars_warn!(format!(
                    "Compression must be one of {{'uncompressed', 'deflate', 'snappy'}}, got {e}. Using defaults."
                ));
                None
            },
        },
        _ => None,
    }
}

#[jni_fn("org.polars.scala.polars.internal.jni.io.write$")]
pub fn writeAvro(
    mut env: JNIEnv,
    _object: JObject,
    df_ptr: *mut DataFrame,
    filePath: JString,
    options: JString,
) {
    let mut options = parse_json_to_options(&mut env, options).unwrap();

    let record_name = options.remove("write_avro_record_name");

    let overwrite_mode = options
        .remove("write_mode")
        .map(|s| matches!(s.to_lowercase().as_str(), "overwrite"))
        .unwrap_or(false);

    let compression = options.remove("write_compression");

    let (mut dataframe, writer): (DataFrame, DynWriter) =
        get_df_and_writer(&mut env, df_ptr, filePath, overwrite_mode, options).unwrap();

    let avro_compression = parse_avro_compression(compression);

    let mut avro_writer = AvroWriter::new(writer).with_compression(avro_compression);

    if let Some(value) = record_name {
        avro_writer = avro_writer.with_name(value)
    }

    avro_writer.finish(&mut dataframe).unwrap();
}
