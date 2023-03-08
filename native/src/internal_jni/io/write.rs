#![allow(non_snake_case)]

use std::collections::HashMap;

use futures::SinkExt;
use jni::JNIEnv;
use jni::objects::{JObject, JString};
use jni::sys::{jboolean, jint, jlong, JNI_TRUE};
use jni_fn::jni_fn;
use object_store::DynObjectStore;
use object_store::path::Path;
use polars::export::arrow::io::parquet::write as write_parquet;
use polars::prelude::*;
use tokio;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use url::Url;

use crate::internal_jni::utils::*;
use crate::j_data_frame::JDataFrame;
use crate::storage_config::StorageOptions;
use crate::utils::{PathError, WriteModes};
use crate::utils::write_utils::{
    build_storage, get_encodings, parse_parquet_compression, parse_write_mode,
};

#[tokio::main(flavor = "current_thread")]
async fn write_files(
    object_store: &DynObjectStore,
    prefix: Path,
    url: Url,
    write_opts: write_parquet::WriteOptions,
    mut data_frame: DataFrame,
    write_mode: WriteModes,
) {
    let meta = object_store.head(&prefix).await;

    let ensure_write_mode =
        if meta.is_ok() && write_mode == WriteModes::ErrorIfExists {
            Err(PathError::FileAlreadyExists(String::from(url.as_str())))
        } else {
            Ok(())
        };

    ensure_write_mode.expect("Error encountered");

    data_frame.rechunk();

    let schema = data_frame.schema().to_arrow();
    let encodings = get_encodings(&schema);
    let mut iter = data_frame.iter_chunks();

    let (_id, writer) = object_store
        .clone()
        .put_multipart(&prefix.clone())
        .await
        .expect("Error encountered while opening destination");

    let mut sink =
        write_parquet::FileSink::try_new(writer.compat_write(), schema, encodings, write_opts)
            .expect("Error encountered while creating file sink");

    while let Some(chunk) = iter.next() {
        sink.feed(chunk)
            .await
            .expect("Error encountered while feeding chunk")
    }

    sink.close()
        .await
        .expect("Error encountered while closing file sink");
}

#[jni_fn("org.polars.scala.polars.internal.jni.io.write$")]
pub fn writeParquet(
    env: JNIEnv,
    _object: JObject,
    df_ptr: jlong,
    filePath: JString,
    writeStats: jboolean,
    compression: JString,
    compressionLevel: jint,
    options: JString,
    writeMode: JString,
) {
    let this_path = get_file_path(env, filePath);

    let compression_str = get_string(
        env,
        compression,
        "Unable to get/ convert compression string to UTF8.",
    );

    let write_mode_str = get_string(
        env,
        writeMode,
        "Unable to get/ convert write mode string to UTF8.",
    );
    let compression_level = if compressionLevel.is_negative() {
        None
    } else {
        Some(compressionLevel as i32)
    };

    let compression = parse_parquet_compression(&compression_str, compression_level)
        .expect("Unable to parse the provided compression argument(s)");

    let write_mode = parse_write_mode(&write_mode_str)
        .expect("Unable to parse the provided write mode argument");

    let options: StorageOptions = if options.is_null() {
        StorageOptions(HashMap::new())
    } else {
        let json = get_string(env, options, "Unable to get/ convert storage options");
        let options_map: HashMap<String, String> = serde_json::from_str(&json).unwrap();
        StorageOptions(options_map)
    };

    let write_options = write_parquet::WriteOptions {
        write_statistics: writeStats == JNI_TRUE,
        version: write_parquet::Version::V2,
        compression,
        data_pagesize_limit: None,
    };

    let j_df = unsafe { &mut *(df_ptr as *mut JDataFrame) };
    let data_frame = j_df.to_owned().df;

    let object_store = build_storage(this_path.clone(), options)
        .expect("Unable to instantiate object store from provided path");

    write_files(
        object_store.storage.as_ref(),
        object_store.path,
        object_store.url,
        write_options,
        data_frame,
        write_mode,
    );
}
