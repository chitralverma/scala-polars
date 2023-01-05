use jni::objects::{GlobalRef, JObject};
use jni::sys::jlong;
use jni::JNIEnv;
use polars::prelude::LazyFrame;

use crate::j_data_frame::JDataFrame;

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
        let df = ldf.collect().expect("Unable to collect DataFrame.");

        let global_ref = env.new_global_ref(callback_obj).unwrap();
        let j_df = JDataFrame::new(df, global_ref);

        Box::into_raw(Box::new(j_df)) as jlong
    }
}
