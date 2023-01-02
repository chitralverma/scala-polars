use jni::objects::{JObject, JString};
use jni::sys::{jobject, jstring};
use jni::JNIEnv;

#[no_mangle]
pub unsafe extern "system" fn Java_org_polars_scala_polars_internal_jni_io_Scan_00024__1scanParquet(
    env: JNIEnv,
    object: JObject,
    path: jstring,
) -> jobject {
    let binding = env
        .get_string(JString::from_raw(path))
        .expect("Unable to get/ convert raw path to UTF8.");

    let this_path = binding
        .to_str()
        .expect("Unable to get/ convert raw path to UTF8.");

    // TODO implement scan_parquet, returning void for now.
    println!("path {:?}", this_path);
    JObject::null().into_raw()
}
