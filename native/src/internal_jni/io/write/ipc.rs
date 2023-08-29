#![allow(non_snake_case)]

use arrow2::io::ipc::write as write_ipc;
use futures::SinkExt;
use jni::objects::{JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use jni_fn::jni_fn;
use object_store::path::Path;
use polars::prelude::*;
use tokio;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use url::Url;
use write_ipc::{Compression, WriteOptions};

use crate::internal_jni::utils::*;
use crate::j_data_frame::JDataFrame;
use crate::utils::write_utils::{
    build_storage, ensure_write_mode, parse_json_to_options, parse_write_mode, ObjectStoreRef,
};
use crate::utils::{PathError, WriteModes};

#[tokio::main(flavor = "current_thread")]
async fn write_files(
    object_store: ObjectStoreRef,
    prefix: Path,
    url: Url,
    write_opts: WriteOptions,
    mut data_frame: DataFrame,
    write_mode: WriteModes,
) {
    let meta = object_store.head(&prefix).await;
    ensure_write_mode(meta, url, write_mode).expect("Error encountered");

    let re_chunked_df = data_frame.align_chunks();

    let schema = re_chunked_df.schema().to_arrow();
    let iter = re_chunked_df.iter_chunks();

    let (_id, writer) = object_store
        .put_multipart(&prefix.clone())
        .await
        .expect("Error encountered while opening destination");

    let mut sink =
        write_ipc::file_async::FileSink::new(writer.compat_write(), schema, None, write_opts);

    for chunk in iter {
        sink.feed(chunk.into())
            .await
            .expect("Error encountered while feeding chunk")
    }

    sink.close()
        .await
        .expect("Error encountered while closing file sink");
}

#[jni_fn("org.polars.scala.polars.internal.jni.io.write$")]
pub fn writeIPC(
    mut env: JNIEnv,
    _object: JObject,
    df_ptr: jlong,
    filePath: JString,
    compression: JString,
    options: JString,
    writeMode: JString,
) {
    let this_path = get_file_path(&mut env, filePath);

    let compression_str = get_string(
        &mut env,
        compression,
        "Unable to get/ convert compression string to UTF8.",
    );

    let compression = parse_ipc_compression(&compression_str)
        .expect("Unable to parse the provided compression argument(s)");

    let write_mode = parse_write_mode(&mut env, writeMode);

    let options = parse_json_to_options(&mut env, options);

    let write_options = WriteOptions { compression };

    let j_df = unsafe { &mut *(df_ptr as *mut JDataFrame) };
    let data_frame = j_df.to_owned().df;

    let (object_store_ref, url, path) = build_storage(this_path.clone(), options)
        .expect("Unable to instantiate object store from provided path");

    write_files(
        object_store_ref,
        path,
        url,
        write_options,
        data_frame,
        write_mode,
    );
}

fn parse_ipc_compression(compression: &str) -> Result<Option<Compression>, PathError> {
    let parsed = match compression {
        "uncompressed" => None,

        "lz4" => Some(Compression::LZ4),

        "zstd" => Some(Compression::ZSTD),

        e => {
            return Err(PathError::Generic(format!(
                "Compression must be one of {{'uncompressed', 'lz4', 'zstd'}}, got {e}",
            )));
        }
    };

    Ok(parsed)
}
