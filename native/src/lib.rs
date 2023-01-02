use jni::objects::JObject;
use jni::sys::jstring;
use jni::JNIEnv;

pub mod scan;

const POLARS_VERSION: &str = "0.26.1";

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_Polars_00024_version(
    env: JNIEnv,
    object: JObject,
) -> jstring {
    env.new_string(POLARS_VERSION)
        .expect("Unable to get Polars version.")
        .into_raw()
}
