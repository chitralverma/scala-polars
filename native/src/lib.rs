#![allow(non_snake_case)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::expect_fun_call)]

use anyhow::Context;
use internal_jni::utils::{j_string_to_string, string_to_j_string};
use jni::JNIEnv;
use jni::objects::{JObject, JString};
use jni::sys::{JNI_TRUE, jboolean, jstring};
use jni_fn::jni_fn;
use utils::error::ResultExt;

pub mod internal_jni;
pub mod utils;

#[jni_fn("com.github.chitralverma.polars.internal.jni.common$")]
pub fn version(mut env: JNIEnv, _object: JObject) -> jstring {
    let cargo_toml_raw = include_str!("../Cargo.toml");
    let cargo_toml_res: anyhow::Result<toml::Table> =
        toml::from_str(cargo_toml_raw).context("context");

    cargo_toml_res
        .map(|cargo_toml| {
            let polars_version = cargo_toml
                .get("dependencies")
                .and_then(|v| v.get("polars"))
                .and_then(|v| v.get("version"));

            let polars_version = match polars_version {
                Some(toml::Value::String(s)) => s.as_str(),
                _ => "unknown",
            };

            string_to_j_string(&mut env, polars_version, None::<&str>)
        })
        .context("Failed to get polars_rs version")
        .unwrap_or_throw(&mut env)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.common$")]
pub fn setConfigs(mut env: JNIEnv, _object: JObject, options: JObject) -> jboolean {
    let map = env
        .get_map(&options)
        .context("Failed to get mapping to rename columns")
        .unwrap_or_throw(&mut env);

    let mut map_iterator = map
        .iter(&mut env)
        .context("Failed to get mapping to rename columns")
        .unwrap_or_throw(&mut env);

    while let Ok(Some((key, value))) = map_iterator.next(&mut env) {
        let key_str = j_string_to_string(
            &mut env,
            &JString::from(key),
            Some("Failed to parse the provided config key as string"),
        );

        let value_str = j_string_to_string(
            &mut env,
            &JString::from(value),
            Some("Failed to parse the provided config value as string"),
        );

        unsafe { std::env::set_var(key_str, value_str) };
    }

    JNI_TRUE
}
