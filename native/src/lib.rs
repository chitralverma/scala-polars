use jni::objects::JObject;
use jni::sys::jstring;
use jni::JNIEnv;

pub mod internal_jni;
pub mod j_data_frame;
pub mod j_expr;
pub mod j_lazy_frame;

const POLARS_VERSION: &str = "0.26.1";

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_common_00024__1version(
    env: JNIEnv,
    _object: JObject,
) -> jstring {
    let version_str = env
        .new_string(POLARS_VERSION)
        .expect("Unable to get Polars version.");

    version_str.into_raw()
}
