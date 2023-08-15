#![allow(non_snake_case)]

use jni::objects::{JObject, JString};
use jni::sys::jstring;
use jni::sys::{jboolean, JNI_TRUE};
use jni::JNIEnv;
use jni_fn::jni_fn;

pub mod internal_jni;
pub mod j_data_frame;
pub mod j_expr;
pub mod j_lazy_frame;
pub mod utils;

#[jni_fn("org.polars.scala.polars.internal.jni.common$")]
pub fn version(mut _env: JNIEnv, _object: JObject) -> jstring {
    let cargo_toml_raw = include_str!("../Cargo.toml");
    let cargo_toml: toml::Table = toml::from_str(cargo_toml_raw).unwrap();
    let polars_version = cargo_toml
        .get("dependencies")
        .and_then(|v| v.get("polars"))
        .and_then(|v| v.get("version"));

    let polars_version = match polars_version {
        Some(toml::Value::String(s)) => s.as_str(),
        _ => "unknown",
    };

    let version_str = _env
        .new_string(polars_version)
        .expect("Unable to get Polars version.");
    version_str.into_raw()
}

#[jni_fn("org.polars.scala.polars.internal.jni.common$")]
pub fn setConfigs(mut _env: JNIEnv, _object: JObject, options: JObject) -> jboolean {
    let opts = _env
        .get_map(&options)
        .expect("Unable to get provided options.");

    let mut iterator = opts
        .iter(&mut _env)
        .expect("The provided options are not iterable.");

    while let Ok(Some((key, value))) = iterator.next(&mut _env) {
        let key_str: String = _env
            .get_string(&JString::from(key))
            .expect("Invalid Config key.")
            .into();

        let value_str: String = _env
            .get_string(&JString::from(value))
            .expect("Invalid Config value.")
            .into();

        std::env::set_var(key_str, value_str);
    }

    JNI_TRUE
}
