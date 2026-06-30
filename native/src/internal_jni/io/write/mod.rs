pub mod avro;
pub mod csv;
pub mod ipc;
pub mod json;
pub mod parquet;

use std::sync::Arc;

use anyhow::Context;
use jni::Env;
use jni::objects::JString;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use polars::io::cloud::cloud_writer::{CloudWriter, CloudWriterIoTraitWrap};
use polars::io::cloud::{CloudOptions, build_object_store, object_path_from_str};
use polars::io::configs::{upload_chunk_size, upload_concurrency};
use polars::io::utils::file::WriteableTrait;
use polars::prelude::*;
use polars_core::runtime::ASYNC;

use super::{get_file_path, parse_cloud_options};

/// Parses the shared `write_mode` option into an overwrite flag, removing the key from the map.
/// Returns `true` only when the value equals `overwrite` (case-insensitive); otherwise `false`.
pub(crate) fn parse_overwrite_mode(options: &mut PlHashMap<String, String>) -> bool {
    options
        .remove("write_mode")
        .map(|s| matches!(s.to_lowercase().as_str(), "overwrite"))
        .unwrap_or(false)
}

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
            polars_err!(ComputeError: "File already exists at the provided location `{uri}` and `write_mode` option is not set to `overwrite` "),
        ),
        _ => Ok(()),
    }
}

async fn create_cloud_writer(
    uri: &str,
    cloud_options: Option<&CloudOptions>,
    overwrite_mode: bool,
) -> PolarsResult<CloudWriterIoTraitWrap> {
    let (cloud_location, object_store) =
        build_object_store(uri.into(), cloud_options, false).await?;
    let dyn_store = object_store.to_dyn_object_store().await;
    ensure_write_mode(
        &dyn_store,
        uri,
        cloud_location.prefix.as_ref(),
        overwrite_mode,
    )
    .await?;

    let mut cloud_writer = CloudWriter::new(
        object_store,
        object_path_from_str(&cloud_location.prefix)?,
        upload_chunk_size(),
        upload_concurrency(),
        None,
    );
    cloud_writer.start().await?;

    Ok(CloudWriterIoTraitWrap::from(cloud_writer))
}

/// Writes a DataFrame to `file_path`, finalizing (committing) the upload afterwards. The
/// `write` closure applies the format-specific writer to the opened cloud writer.
pub(crate) fn write_dataframe<F>(
    env: &mut Env,
    mut dataframe: DataFrame,
    file_path: &JString,
    overwrite_mode: bool,
    options: PlHashMap<String, String>,
    format: &str,
    write: F,
) -> anyhow::Result<()>
where
    F: FnOnce(&mut CloudWriterIoTraitWrap, &mut DataFrame) -> PolarsResult<()>,
{
    let full_path = get_file_path(env, file_path)?;
    let uri = PlRefPath::new(full_path);

    let cloud_options = parse_cloud_options(uri.scheme(), options)?;
    let mut writer: CloudWriterIoTraitWrap = ASYNC
        .block_on(async {
            create_cloud_writer(uri.as_str(), cloud_options.as_ref(), overwrite_mode).await
        })
        .context("Failed to create writer")?;

    write(&mut writer, &mut dataframe).with_context(|| format!("Failed to write {format} data"))?;

    writer
        .close()
        .with_context(|| format!("Failed to finalize {format} data"))?;

    Ok(())
}
