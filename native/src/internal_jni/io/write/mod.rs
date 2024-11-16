pub mod avro;
pub mod csv;
pub mod ipc;
pub mod json;
pub mod parquet;

use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use jni::objects::JString;
use jni::sys::jlong;
use jni::JNIEnv;
use object_store::path::Path;
use object_store::ObjectStore;
use polars::io::cloud::{build_object_store, CloudOptions, CloudWriter};
use polars::io::pl_async::get_runtime;
use polars::prelude::*;

use crate::internal_jni::utils::{get_file_path, get_string};
use crate::j_data_frame::JDataFrame;

pub type ObjectStoreRef = Arc<dyn ObjectStore>;
pub type DynWriter = Box<dyn Write>;

fn parse_json_to_options(
    env: &mut JNIEnv,
    options: JString,
) -> PolarsResult<PlHashMap<String, String>> {
    if options.is_null() {
        Ok(PlHashMap::new())
    } else {
        let json = get_string(env, options, "Unable to get/ convert options");
        serde_json::from_str(&json)
            .map_err(|_| polars_err!(InvalidOperation: "Unable to parse options"))
    }
}

async fn ensure_write_mode(
    object_store_ref: &ObjectStoreRef,
    prefix: &str,
    overwrite_mode: bool,
) -> PolarsResult<()> {
    let meta = object_store_ref.head(&Path::from(prefix)).await;
    match meta {
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(PolarsError::IO {
            error: Arc::new(e.into()),
            msg: Some("Cannot connect to object store".into()),
        }),
        Ok(_) if !overwrite_mode => {
            Err(polars_err!(ComputeError: "File already exists at the given prefix: {}", prefix))
        },
        _ => Ok(()),
    }
}

async fn create_cloud_writer(
    uri: &str,
    cloud_options: Option<&CloudOptions>,
    overwrite_mode: bool,
) -> PolarsResult<CloudWriter> {
    let (cloud_location, object_store) = build_object_store(uri, cloud_options, false).await?;
    ensure_write_mode(
        &object_store,
        cloud_location.prefix.as_ref(),
        overwrite_mode,
    )
    .await?;

    let cloud_writer = CloudWriter::new_with_object_store(
        object_store.clone(),
        cloud_location.prefix.clone().into(),
    )?;

    Ok(cloud_writer)
}

fn get_df_and_writer(
    env: &mut JNIEnv,
    df_ptr: jlong,
    filePath: JString,
    overwrite_mode: bool,
    writer_options: PlHashMap<String, String>,
) -> PolarsResult<(DataFrame, DynWriter)> {
    let full_path = get_file_path(env, filePath);
    let uri = full_path.as_str();

    let writer: DynWriter = match (overwrite_mode, is_cloud_url(uri)) {
        (overwrite_mode, true) => {
            let cloud_options = CloudOptions::from_untyped_config(uri, &writer_options)?;
            let cloud_writer = get_runtime().block_on_potential_spawn(async {
                create_cloud_writer(uri, Some(cloud_options).as_ref(), overwrite_mode).await
            })?;

            Box::new(cloud_writer)
        },
        (true, false) => {
            let file = File::create(uri)?;
            Box::new(file)
        },
        _ => {
            let file = File::create_new(uri)?;
            Box::new(file)
        },
    };

    let j_df = unsafe { &mut *(df_ptr as *mut JDataFrame) };
    let dataframe = j_df.to_owned().df;

    Ok((dataframe, writer))
}
