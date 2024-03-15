#![allow(non_snake_case)]

use futures::SinkExt;
use jni::objects::{JObject, JString};
use jni::sys::{jboolean, jint, jlong, JNI_TRUE};
use jni::JNIEnv;
use jni_fn::jni_fn;
use object_store::path::Path;
use polars::prelude::*;
use polars_parquet::write as write_parquet;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use url::Url;
use write_parquet::{BrotliLevel, CompressionOptions, GzipLevel, ZstdLevel};

use crate::internal_jni::utils::*;
use crate::j_data_frame::JDataFrame;
use crate::utils::write_utils::{
    build_storage, ensure_write_mode, get_encodings, parse_json_to_options, parse_write_mode,
    ObjectStoreRef,
};
use crate::utils::{PathError, WriteModes};

#[tokio::main(flavor = "current_thread")]
async fn write_files(
    object_store: ObjectStoreRef,
    prefix: Path,
    url: Url,
    write_opts: write_parquet::WriteOptions,
    mut data_frame: DataFrame,
    write_mode: WriteModes,
) {
    let meta = object_store.head(&prefix).await;
    ensure_write_mode(meta, url, write_mode).expect("Error encountered");

    let re_chunked_df = data_frame.align_chunks();

    let schema = re_chunked_df.schema().to_arrow(true);
    let encodings = get_encodings(&schema);
    let iter = re_chunked_df.iter_chunks(true);

    let (_id, writer) = object_store
        .put_multipart(&prefix.clone())
        .await
        .expect("Error encountered while opening destination");

    let mut sink =
        write_parquet::FileSink::try_new(writer.compat_write(), schema, encodings, write_opts)
            .expect("Error encountered while creating file sink");

    for chunk in iter {
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
    mut env: JNIEnv,
    _object: JObject,
    df_ptr: jlong,
    filePath: JString,
    writeStats: jboolean,
    compression: JString,
    compressionLevel: jint,
    options: JString,
    writeMode: JString,
) {
    let this_path = get_file_path(&mut env, filePath);

    let compression_str = get_string(
        &mut env,
        compression,
        "Unable to get/ convert compression string to UTF8.",
    );
    let compression_level = if compressionLevel.is_negative() {
        None
    } else {
        Some(compressionLevel)
    };

    let compression = parse_parquet_compression(&compression_str, compression_level)
        .expect("Unable to parse the provided compression argument(s)");

    let write_mode = parse_write_mode(&mut env, writeMode);

    let options = parse_json_to_options(&mut env, options);

    let write_options = write_parquet::WriteOptions {
        write_statistics: writeStats == JNI_TRUE,
        version: write_parquet::Version::V2,
        compression,
        data_pagesize_limit: None,
    };

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

fn parse_parquet_compression(
    compression: &str,
    compression_level: Option<i32>,
) -> Result<CompressionOptions, PathError> {
    let parsed = match compression {
        "uncompressed" => CompressionOptions::Uncompressed,

        "snappy" => CompressionOptions::Snappy,

        "lz4" => CompressionOptions::Lz4Raw,

        "lzo" => CompressionOptions::Lzo,

        "gzip" => CompressionOptions::Gzip(
            compression_level
                .map(|lvl| {
                    GzipLevel::try_new(lvl as u8).map_err(|e| PathError::Generic(format!("{e:?}")))
                })
                .transpose()?,
        ),

        "brotli" => CompressionOptions::Brotli(
            compression_level
                .map(|lvl| {
                    BrotliLevel::try_new(lvl as u32)
                        .map_err(|e| PathError::Generic(format!("{e:?}")))
                })
                .transpose()?,
        ),

        "zstd" => CompressionOptions::Zstd(
            compression_level
                .map(|lvl| {
                    ZstdLevel::try_new(lvl).map_err(|e| PathError::Generic(format!("{e:?}")))
                })
                .transpose()?,
        ),

        e => {
            return Err(PathError::Generic(format!(
                "Compression must be one of {{'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'lz4', 'zstd'}}, got {e}",
            )));
        },
    };
    Ok(parsed)
}
