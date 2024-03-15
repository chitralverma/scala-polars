use jni::objects::{GlobalRef, JObject};
use jni::sys::jlong;
use jni::JNIEnv;
use polars::prelude::*;

use crate::internal_jni::utils::df_to_ptr;

#[derive(Clone)]
pub struct JDataFrame {
    pub df: DataFrame,
    _callback: GlobalRef,
}

impl JDataFrame {
    pub fn new(df: DataFrame, callback: GlobalRef) -> JDataFrame {
        JDataFrame {
            df,
            _callback: callback,
        }
    }

    pub fn show(&self) {
        println!("{:?}", self.df)
    }

    pub fn limit(&self, env: &mut JNIEnv, callback_obj: JObject, n: usize) -> jlong {
        let df = self.df.clone().head(Some(n));

        df_to_ptr(env, callback_obj, Ok(df))
    }

    pub fn tail(&self, env: &mut JNIEnv, callback_obj: JObject, n: usize) -> jlong {
        let df = self.df.clone().tail(Some(n));

        df_to_ptr(env, callback_obj, Ok(df))
    }
}
