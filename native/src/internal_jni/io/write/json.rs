#![allow(non_snake_case)]

use jni::objects::JObject;
use jni::sys::{_jobject, jboolean, jlong};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::prelude::*;

use crate::j_data_frame::JDataFrame;

#[jni_fn("org.polars.scala.polars.internal.jni.io.write$")]
pub fn toJson(
    env: JNIEnv,
    _object: JObject,
    df_ptr: jlong,
    pretty: jboolean,
    row_oriented: jboolean,
) -> *mut _jobject {
    let j_df = unsafe { &mut *(df_ptr as *mut JDataFrame) };
    let mut data_frame = j_df.to_owned().df;

    let mut df = data_frame.as_single_chunk_par();

    let mut buf: Vec<u8> = Vec::new();
    match (pretty == 1, row_oriented == 1) {
        (_, true) => JsonWriter::new(&mut buf)
            .with_json_format(JsonFormat::Json)
            .finish(&mut df),
        (true, _) => serde_json::to_writer_pretty(&mut buf, &df)
            .map_err(|e| polars_err!(ComputeError: "{e}")),
        (false, _) => {
            serde_json::to_writer(&mut buf, &df).map_err(|e| polars_err!(ComputeError: "{e}"))
        },
    }
    .expect("Unable to format JSON");

    let rust_string = String::from_utf8(buf).unwrap();

    let output = env
        .new_string(rust_string)
        .expect("Couldn't create Java string!");

    output.into_raw()
}
