#![allow(non_snake_case)]

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::error::Result as ArrowResult;
use arrow2::io::avro as arrow2_avro;
use arrow2_avro::avro_schema::file::{Block, CompressedBlock, Compression};
use arrow2_avro::avro_schema::schema::Record;
use arrow2_avro::avro_schema::write::compress;
use arrow2_avro::avro_schema::write_async::{write_block, write_metadata};
use arrow2_avro::write as write_avro;
use jni::objects::{JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use jni_fn::jni_fn;
use object_store::path::Path;
use polars::prelude::*;
use tokio;
use tokio::io::AsyncWriteExt;
use url::Url;

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
    compression: Option<Compression>,
    mut data_frame: DataFrame,
    write_mode: WriteModes,
) -> Result<(), PathError> {
    let meta = object_store.head(&prefix).await;
    ensure_write_mode(meta, url, write_mode)?;

    let re_chunked_df = data_frame.align_chunks();

    let schema = re_chunked_df.schema().to_arrow();
    let record = write_avro::to_record(&schema)?;
    let iter = re_chunked_df.iter_chunks();

    let (_, mut writer) = object_store.put_multipart(&prefix.clone()).await?;

    for chunk in iter {
        let compressed_block = serialize_to_block(&chunk, &record, compression)?;
        let mut buffer = vec![];

        write_metadata(&mut buffer, record.clone(), compression).await?;
        write_block(&mut buffer, &compressed_block).await?;

        writer.write_all(&buffer).await?;
        buffer.clear();
    }

    writer.shutdown().await.map_err(PathError::from)
}

pub(super) fn serialize_to_block<R: AsRef<dyn Array>>(
    columns: &Chunk<R>,
    record: &Record,
    compression: Option<Compression>,
) -> ArrowResult<CompressedBlock> {
    let mut serializers = columns
        .arrays()
        .iter()
        .map(|x| x.as_ref())
        .zip(record.fields.iter())
        .map(|(array, field)| write_avro::new_serializer(array, &field.schema))
        .collect::<Vec<_>>();
    let mut block = Block::new(columns.len(), vec![]);

    write_avro::serialize(&mut serializers, &mut block);

    let mut compressed_block = CompressedBlock::default();

    compress(&mut block, &mut compressed_block, compression)?;

    Ok(compressed_block)
}

#[jni_fn("org.polars.scala.polars.internal.jni.io.write$")]
pub fn writeAvro(
    mut env: JNIEnv,
    _object: JObject,
    df_ptr: jlong,
    filePath: JString,
    compression: JString,
    options: JString,
    writeMode: JString,
) {
    let this_path = get_file_path(&mut env, filePath);

    let write_mode = parse_write_mode(&mut env, writeMode);
    let options = parse_json_to_options(&mut env, options);
    let compression = parse_avro_compression(&mut env, compression)
        .expect("Unable to parse the provided compression argument(s)");

    let j_df = unsafe { &mut *(df_ptr as *mut JDataFrame) };
    let data_frame = j_df.to_owned().df;

    let (object_store_ref, url, path) = build_storage(this_path.clone(), options)
        .expect("Unable to instantiate object store from provided path");

    write_files(
        object_store_ref,
        path,
        url,
        compression,
        data_frame,
        write_mode,
    )
    .expect("Error encountered while writing Avro files");
}

fn parse_avro_compression(
    env: &mut JNIEnv,
    compression: JString,
) -> Result<Option<Compression>, PathError> {
    let compression_str = get_string(
        env,
        compression,
        "Unable to get/ convert compression string to UTF8.",
    );

    let parsed = match compression_str.as_str() {
        "uncompressed" => None,

        "deflate" => Some(Compression::Deflate),

        "snappy" => Some(Compression::Snappy),

        e => {
            return Err(PathError::Generic(format!(
                "Compression must be one of {{'uncompressed', 'deflate', 'snappy'}}, got {e}",
            )));
        }
    };

    Ok(parsed)
}
