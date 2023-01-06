use jni::objects::JObject;
use jni::sys::jlong;
use jni::JNIEnv;

use crate::j_data_frame::JDataFrame;

#[no_mangle]
pub unsafe extern "system" fn Java_org_polars_scala_polars_internal_jni_data_1frame_00024_show(
    _env: JNIEnv,
    _object: JObject,
    ptr: jlong,
) {
    let j_df = &mut *(ptr as *mut JDataFrame);
    j_df.show()
}

#[no_mangle]
pub unsafe extern "system" fn Java_org_polars_scala_polars_internal_jni_data_1frame_00024_count(
    _env: JNIEnv,
    _object: JObject,
    ptr: jlong,
) -> jlong {
    let j_df = &mut *(ptr as *mut JDataFrame);
    let count = j_df.df.shape().0 as i64;

    jlong::from(count)
}
