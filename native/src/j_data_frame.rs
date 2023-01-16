use crate::internal_jni::utils::df_to_ptr;
use jni::objects::{GlobalRef, JObject};
use jni::sys::jlong;
use jni::JNIEnv;
use polars::prelude::*;

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

    pub fn select(&self, env: JNIEnv, callback_obj: JObject, exprs: Vec<String>) -> jlong {
        let df_res = self.df.clone().select(exprs);

        df_to_ptr(env, callback_obj, df_res)
    }
}
