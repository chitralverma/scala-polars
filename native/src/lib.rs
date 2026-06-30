#![allow(non_snake_case)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::expect_fun_call)]

use anyhow::Context;
use internal_jni::utils::j_object_ref_to_string;
use jni::objects::{JMap, JObject, JString};
use jni::sys::{JNI_TRUE, jboolean};
use jni::{Env, native_method};
use utils::error::ThrowRuntimeException;

pub mod internal_jni;
pub mod utils;

#[allow(dead_code)]
const VERSION_METHOD: jni::NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.common$",
    error_policy = ThrowRuntimeException,
    extern fn version() -> JString,
};

#[allow(dead_code)]
const SET_CONFIGS_METHOD: jni::NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.common$",
    error_policy = ThrowRuntimeException,
    extern fn set_configs(options: java.util.Map) -> jboolean,
    name = "setConfigs",
};

fn version<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
) -> anyhow::Result<JString<'local>> {
    let cargo_toml_raw = include_str!("../Cargo.toml");
    let cargo_toml: toml::Table =
        toml::from_str(cargo_toml_raw).context("Failed to parse Cargo.toml")?;

    let polars_version = cargo_toml
        .get("dependencies")
        .and_then(|v| v.get("polars"))
        .and_then(|v| v.get("version"));

    let polars_version = match polars_version {
        Some(toml::Value::String(s)) => s.as_str(),
        _ => "unknown",
    };

    JString::from_str(env, polars_version).context("Failed to build version string")
}

fn set_configs<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    options: JMap<'local>,
) -> anyhow::Result<jboolean> {
    let mut map_iterator = options
        .iter(env)
        .context("Failed to get mapping iterator to set configs")?;

    while let Some(entry) = map_iterator
        .next(env)
        .context("Failed to read next entry while setting configs")?
    {
        let key_obj = entry.key(env)?;
        let key_str = j_object_ref_to_string(
            env,
            &key_obj,
            Some("Failed to parse the provided config key as string"),
        )?;

        let value_obj = entry.value(env)?;
        let value_str = j_object_ref_to_string(
            env,
            &value_obj,
            Some("Failed to parse the provided config value as string"),
        )?;

        unsafe { std::env::set_var(key_str, value_str) };
    }
    Ok(JNI_TRUE)
}
