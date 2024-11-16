#![allow(non_snake_case)]

use jni::objects::{JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::prelude::*;

use crate::internal_jni::io::write::{get_df_and_writer, parse_json_to_options, DynWriter};

#[jni_fn("org.polars.scala.polars.internal.jni.io.write$")]
pub fn writeJson(
    mut env: JNIEnv,
    _object: JObject,
    df_ptr: jlong,
    filePath: JString,
    options: JString,
) {
    let mut options = parse_json_to_options(&mut env, options).unwrap();

    let json_format = options
        .remove("write_json_format")
        .and_then(|s| match s.to_lowercase().as_str() {
            "json" => Some(JsonFormat::Json),
            "json_lines" => Some(JsonFormat::JsonLines),
            _ => None,
        })
        .unwrap_or(JsonFormat::Json);

    let overwrite_mode = options
        .remove("write_mode")
        .map(|s| matches!(s.to_lowercase().as_str(), "overwrite"))
        .unwrap_or(false);

    let (mut dataframe, writer): (DataFrame, DynWriter) =
        get_df_and_writer(&mut env, df_ptr, filePath, overwrite_mode, options).unwrap();

    let mut json_writer = JsonWriter::new(writer).with_json_format(json_format);

    json_writer.finish(&mut dataframe).unwrap();
}
