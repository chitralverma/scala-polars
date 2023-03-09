use std::collections::HashMap;
use std::sync::Arc;

use arrow2::datatypes::PhysicalType;
use arrow2::io::parquet::write::{transverse, Encoding};
use jni::objects::JString;
use jni::JNIEnv;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectMeta};
use polars::prelude::{ArrowDataType, ArrowSchema};
use url::Url;

use crate::internal_jni::utils::{get_string, normalize_path};
use crate::storage_config::StorageOptions;
use crate::utils::{PathError, WriteModes};

pub type ObjectStoreRef = Arc<DynObjectStore>;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct PolarsObjectStore {
    pub(crate) storage: ObjectStoreRef,
    pub(crate) url: Url,
    pub(crate) storage_options: StorageOptions,
    pub(crate) path: Path,
}

pub fn build_storage(
    path_str: String,
    options: StorageOptions,
) -> Result<PolarsObjectStore, PathError> {
    let location = path_to_url(&path_str)?.clone();
    let path = normalize_path(std::path::Path::new(location.path()));

    let storage = get_object_store_ref(&(location.clone()), options.clone());

    Ok(PolarsObjectStore {
        storage: storage?,
        url: location.clone(),
        storage_options: options,
        path: Path::from(path.to_string_lossy().as_ref()),
    })
}

fn get_object_store_ref(
    url: &Url,
    options: impl Into<StorageOptions>,
) -> Result<ObjectStoreRef, PathError> {
    let _options = options.into();

    match url.scheme() {
        "file" => Ok(Arc::new(LocalFileSystem::new())),

        "s3" | "s3a" => {
            let store = AmazonS3Builder::from_env()
                .with_url(url.as_ref())
                .try_with_options(&_options.as_s3_options())
                .unwrap()
                .with_allow_http(_options.allow_http())
                .build()
                .unwrap();
            Ok(Arc::new(store))
        }

        "az" | "abfs" | "abfss" | "azure" | "wasb" | "adl" => {
            let store = MicrosoftAzureBuilder::from_env()
                .with_url(url.as_ref())
                .try_with_options(&_options.as_azure_options())
                .unwrap()
                .with_allow_http(_options.allow_http())
                .build()
                .unwrap();
            Ok(Arc::new(store))
        }

        "gs" => {
            let store = GoogleCloudStorageBuilder::from_env()
                .with_url(url.as_ref())
                .try_with_options(&_options.as_gcs_options())
                .unwrap()
                .build()
                .unwrap();
            Ok(Arc::new(store))
        }

        _ => Err(PathError::Generic(format!(
            "unsupported url: {}",
            url.as_str()
        ))),
    }
}

fn path_to_url(path_str: impl AsRef<str>) -> Result<Url, PathError> {
    let path_str = path_str.as_ref();

    if let Ok(url) = Url::parse(&path_str).or_else(|_| Url::from_file_path(path_str)) {
        match url.scheme() {
            "file" => Ok(url),
            _ => Ok({
                let mut new_url = url.clone();
                new_url.set_path(url.path().trim_end_matches('/'));
                new_url
            }),
        }
    } else {
        Err(PathError::InvalidTableLocation(path_str.to_string()))
    }
}

fn encoding_map(data_type: &ArrowDataType) -> Encoding {
    match data_type.to_physical_type() {
        PhysicalType::Dictionary(_) => Encoding::RleDictionary,
        // remaining is plain
        _ => Encoding::Plain,
    }
}

pub fn get_encodings(schema: &ArrowSchema) -> Vec<Vec<Encoding>> {
    schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, encoding_map))
        .collect()
}

pub fn parse_write_mode(write_mode: &str) -> Result<WriteModes, PathError> {
    let parsed = match write_mode {
        "Overwrite" => WriteModes::Overwrite,
        _ => WriteModes::ErrorIfExists,
    };
    Ok(parsed)
}

pub fn ensure_write_mode(
    meta: object_store::Result<ObjectMeta>,
    url: Url,
    write_mode: WriteModes,
) -> Result<(), PathError> {
    if meta.is_ok() && write_mode == WriteModes::ErrorIfExists {
        Err(PathError::FileAlreadyExists(String::from(url.as_str())))
    } else {
        Ok(())
    }
}

pub fn parse_json_to_storage_options(env: JNIEnv, options: JString) -> StorageOptions {
    if options.is_null() {
        StorageOptions(HashMap::new())
    } else {
        let json = get_string(env, options, "Unable to get/ convert storage options");
        StorageOptions::fromJSON(json)
    }
}
