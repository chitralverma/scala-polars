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
mod storage_config;
pub mod utils;

const POLARS_VERSION: &str = "0.26.1";

#[jni_fn("org.polars.scala.polars.internal.jni.common$")]
pub fn version(env: JNIEnv, _object: JObject) -> jstring {
    let version_str = env
        .new_string(POLARS_VERSION)
        .expect("Unable to get Polars version.");

    version_str.into_raw()
}

#[jni_fn("org.polars.scala.polars.internal.jni.common$")]
pub fn setConfigs(_env: JNIEnv, _object: JObject, options: JObject) -> jboolean {
    let opts = _env
        .get_map(options)
        .expect("Unable to get provided options.");
    assert!(!opts.is_null(), "Options cannot be null.");

    opts.iter().unwrap().for_each(|(key, value)| {
        let key_str: String = _env
            .get_string(JString::from(key))
            .expect("Invalid Config key.")
            .into();
        let value_str: String = _env
            .get_string(JString::from(value))
            .expect("Invalid Config value.")
            .into();

        std::env::set_var(key_str, value_str);
    });

    JNI_TRUE
}
