pub mod avro;
pub mod csv;
pub mod ipc;
pub mod json;
pub mod parquet;

use std::sync::Arc;

use anyhow::Context;
use jni::objects::JString;
use jni::JNIEnv;
use object_store::path::Path;
use object_store::ObjectStore;
use polars::io::cloud::{build_object_store, CloudOptions, CloudWriter};
use polars::io::pl_async::get_runtime;
use polars::prelude::*;

use super::get_file_path;
use crate::utils::error::ResultExt;

async fn ensure_write_mode(
    object_store_ref: &Arc<dyn ObjectStore>,
    uri: &str,
    prefix: &str,
    overwrite_mode: bool,
) -> PolarsResult<()> {
    let meta = object_store_ref.head(&Path::from(prefix)).await;
    match meta {
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(PolarsError::IO {
            error: Arc::new(e.into()),
            msg: Some("Failed to connect to object store, recheck the provided options".into()),
        }),
        Ok(_) if !overwrite_mode => Err(
            polars_err!(ComputeError: "File already exists at the provided location `{uri}` and overwrite option is not set"),
        ),
        _ => Ok(()),
    }
}

async fn create_cloud_writer(
    uri: &str,
    cloud_options: Option<&CloudOptions>,
    overwrite_mode: bool,
) -> PolarsResult<CloudWriter> {
    let (cloud_location, object_store) = build_object_store(uri, cloud_options, false).await?;
    let dyn_store = object_store.to_dyn_object_store().await;
    ensure_write_mode(
        &dyn_store,
        uri,
        cloud_location.prefix.as_ref(),
        overwrite_mode,
    )
    .await?;

    let cloud_writer = CloudWriter::new_with_object_store(
        dyn_store.clone(),
        cloud_location.prefix.clone().into(),
    )?;

    Ok(cloud_writer)
}

fn get_df_and_writer(
    env: &mut JNIEnv,
    df_ptr: *mut DataFrame,
    filePath: JString,
    overwrite_mode: bool,
    writer_options: PlHashMap<String, String>,
) -> (DataFrame, CloudWriter) {
    let full_path = get_file_path(env, filePath);
    let uri = full_path.as_str();

    let cloud_options = CloudOptions::from_untyped_config(uri, &writer_options);
    let writer: CloudWriter = get_runtime()
        .block_on_potential_spawn(async {
            create_cloud_writer(uri, cloud_options.ok().as_ref(), overwrite_mode).await
        })
        .context("Failed to create writer")
        .unwrap_or_throw(env);

    let dataframe = unsafe { &*df_ptr }.clone();
    (dataframe, writer)
}
