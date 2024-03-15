use std::collections::HashMap;

use jni::objects::JString;
use jni::JNIEnv;
use object_store::path::Error::InvalidPath;
use object_store::path::Path;
use object_store::{parse_url_opts, DynObjectStore, ObjectMeta};
use polars::prelude::{ArrowDataType, ArrowSchema};
use polars_arrow::datatypes::PhysicalType;
use polars_parquet::write::{transverse, Encoding};
use url::Url;

use crate::internal_jni::utils::get_string;
use crate::utils::{PathError, WriteModes};

pub type ObjectStoreRef = Box<DynObjectStore>;

#[allow(unused)]
#[derive(Debug)]
pub struct PolarsObjectStore {
    pub(crate) storage: ObjectStoreRef,
    pub(crate) url: Url,
    pub(crate) path: Path,
}

pub fn build_storage(
    path_str: String,
    options: HashMap<String, String>,
) -> Result<(ObjectStoreRef, Url, Path), object_store::Error> {
    let location = path_to_url(path_str)?.clone();
    let (storage, path) = parse_url_opts(&(location.clone()), options)?;

    Ok((storage, location.clone(), path))
}

fn path_to_url(path_str: impl AsRef<str>) -> Result<Url, object_store::path::Error> {
    let path_str = path_str.as_ref();

    if let Ok(url) = Url::parse(path_str).or_else(|_| Url::from_file_path(path_str)) {
        match url.scheme() {
            "file" => Ok(url),
            _ => Ok({
                let mut new_url = url.clone();
                new_url.set_path(url.path().trim_end_matches('/'));
                new_url
            }),
        }
    } else {
        Err(InvalidPath {
            path: path_str.into(),
        })
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

pub fn parse_write_mode(env: &mut JNIEnv, writeMode: JString) -> WriteModes {
    let write_mode_str = get_string(
        env,
        writeMode,
        "Unable to get/ convert write mode string to UTF8.",
    );

    match write_mode_str.as_str() {
        "Overwrite" => WriteModes::Overwrite,
        _ => WriteModes::ErrorIfExists,
    }
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

pub fn parse_json_to_options(env: &mut JNIEnv, options: JString) -> HashMap<String, String> {
    if options.is_null() {
        HashMap::new()
    } else {
        let json = get_string(env, options, "Unable to get/ convert storage options");
        serde_json::from_str(&json).unwrap()
    }
}
