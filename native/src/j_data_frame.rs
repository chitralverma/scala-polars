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

    pub fn select(&self, env: JNIEnv, callback_obj: JObject, exprs: Vec<Expr>) -> jlong {
        let df_res = self
            .df
            .clone()
            .lazy()
            .select(exprs)
            .without_optimizations()
            .collect();

        df_to_ptr(env, callback_obj, df_res)
    }

    pub fn filter(&self, env: JNIEnv, callback_obj: JObject, predicate: Expr) -> jlong {
        let df_res = self
            .df
            .clone()
            .lazy()
            .filter(predicate)
            .without_optimizations()
            .collect();

        df_to_ptr(env, callback_obj, df_res)
    }
}
