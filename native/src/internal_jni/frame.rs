#![allow(non_snake_case)]

use jni::objects::ReleaseMode::NoCopyBack;
use jni::objects::{JLongArray, JObject};
use jni::sys::{jlong, jstring};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::export::num::ToPrimitive;
use polars::prelude::*;
use polars_core::utils::concat_df;

use crate::internal_jni::utils::*;
use crate::j_data_frame::JDataFrame;
use crate::j_series::JSeries;

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub fn schemaString(mut _env: JNIEnv, _object: JObject, ldf_ptr: jlong) -> jstring {
    let j_df = unsafe { &mut *(ldf_ptr as *mut JDataFrame) };
    let schema_string = serde_json::to_string(&j_df.df.schema().to_arrow(CompatLevel::oldest())).unwrap();

    _env.new_string(schema_string)
        .expect("Unable to get/ convert Schema to UTF8.")
        .into_raw()
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub fn show(mut _env: JNIEnv, _object: JObject, ptr: jlong) {
    let j_df = unsafe { &mut *(ptr as *mut JDataFrame) };
    j_df.show()
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub fn count(mut _env: JNIEnv, _object: JObject, ptr: jlong) -> jlong {
    let j_df = unsafe { &mut *(ptr as *mut JDataFrame) };

    j_df.df.shape().0 as i64
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub fn concatDataFrames(mut env: JNIEnv, object: JObject, inputs: JLongArray) -> jlong {
    let arr = unsafe { env.get_array_elements(&inputs, NoCopyBack).unwrap() };

    let vec: Vec<DataFrame> = unsafe {
        std::slice::from_raw_parts(arr.as_ptr(), arr.len())
            .to_vec()
            .iter()
            .map(|p| p.to_i64().unwrap())
            .map(|ptr| {
                let j_ldf = &mut *(ptr as *mut JDataFrame);
                j_ldf.to_owned().df
            })
            .collect()
    };

    let concat_ldf = concat_df(vec.as_slice());
    df_to_ptr(&mut env, object, concat_ldf)
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub fn toLazy(mut env: JNIEnv, object: JObject, ptr: jlong) -> jlong {
    let j_df = unsafe { &mut *(ptr as *mut JDataFrame) };
    let ldf = j_df.df.clone().lazy();

    ldf_to_ptr(&mut env, object, Ok(ldf))
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub fn limit(mut env: JNIEnv, object: JObject, ptr: jlong, n: jlong) -> jlong {
    let j_df = unsafe { &mut *(ptr as *mut JDataFrame) };

    j_df.limit(&mut env, object, n as usize)
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub fn tail(mut env: JNIEnv, object: JObject, ptr: jlong, n: jlong) -> jlong {
    let j_df = unsafe { &mut *(ptr as *mut JDataFrame) };

    j_df.tail(&mut env, object, n as usize)
}

#[jni_fn("org.polars.scala.polars.internal.jni.data_frame$")]
pub fn fromSeries(mut env: JNIEnv, callback_obj: JObject, ptrs: JLongArray) -> jlong {
    let arr = unsafe { env.get_array_elements(&ptrs, NoCopyBack).unwrap() };
    let data: Vec<Column> = unsafe {
        std::slice::from_raw_parts(arr.as_ptr(), arr.len())
            .to_vec()
            .iter()
            .map(|p| p.to_i64().unwrap())
            .map(|ptr| {
                let j_series = &mut *(ptr as *mut JSeries);
                j_series.to_owned().series
            })
            .map(|s| s.into_column())
            .collect()
    };

    let df = DataFrame::new(data);
    df_to_ptr(&mut env, callback_obj, df)
}
