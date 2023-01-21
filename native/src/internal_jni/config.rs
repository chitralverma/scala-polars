use jni::objects::{JObject, JString};
use jni::sys::{jboolean, JNI_TRUE};
use jni::JNIEnv;

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_config_00024__1setConfigs(
    _env: JNIEnv,
    _object: JObject,
    options: JObject,
) -> jboolean {
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
