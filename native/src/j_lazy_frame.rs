use jni::objects::{GlobalRef, JObject};
use jni::sys::jlong;
use jni::JNIEnv;
use polars::prelude::{Expr, LazyFrame};

use crate::internal_jni::utils::{df_to_ptr, ldf_to_ptr};

#[derive(Clone)]
pub struct JLazyFrame {
    pub ldf: LazyFrame,
    _callback: GlobalRef,
}

impl JLazyFrame {
    pub fn new(ldf: LazyFrame, callback: GlobalRef) -> JLazyFrame {
        JLazyFrame {
            ldf,
            _callback: callback,
        }
    }

    pub fn collect(&self, env: JNIEnv, callback_obj: JObject) -> jlong {
        let ldf = self.ldf.clone();
        let df = ldf.collect();

        df_to_ptr(env, callback_obj, df)
    }

    pub fn select(&self, env: JNIEnv, callback_obj: JObject, exprs: Vec<Expr>) -> jlong {
        let ldf = self.ldf.clone().select(exprs);

        ldf_to_ptr(env, callback_obj, Ok(ldf))
    }
}
