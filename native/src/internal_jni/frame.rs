#![allow(non_snake_case)]

use jni::objects::JObject;
use jni::objects::ReleaseMode::NoCopyBack;
use jni::sys::{jlong, jlongArray};
use jni::JNIEnv;

use polars::export::num::ToPrimitive;
use polars::prelude::*;
use polars_core::utils::concat_df;

use crate::internal_jni::utils::*;
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

#[no_mangle]
pub unsafe extern "system" fn Java_org_polars_scala_polars_internal_jni_common_00024__1concatDataFrames(
    env: JNIEnv,
    object: JObject,
    inputs: jlongArray,
) -> jlong {
    let arr = env.get_long_array_elements(inputs, NoCopyBack).unwrap();

    let vec: Vec<DataFrame> =
        std::slice::from_raw_parts(arr.as_ptr(), arr.size().unwrap() as usize)
            .to_vec()
            .iter()
            .map(|p| p.to_i64().unwrap())
            .map(|ptr| {
                let j_ldf = &mut *(ptr as *mut JDataFrame);
                j_ldf.to_owned().df
            })
            .collect();

    let concat_ldf = concat_df(vec.as_slice());
    df_to_ptr(env, object, concat_ldf)
}
