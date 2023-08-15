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

    pub fn collect(&self, env: &mut JNIEnv, callback_obj: JObject) -> jlong {
        let ldf = self.ldf.clone();
        let df = ldf.collect();

        df_to_ptr(env, callback_obj, df)
    }

    pub fn select(&self, env: &mut JNIEnv, callback_obj: JObject, exprs: Vec<Expr>) -> jlong {
        let ldf = self.ldf.clone().select(exprs);

        ldf_to_ptr(env, callback_obj, Ok(ldf))
    }

    pub fn filter(&self, env: &mut JNIEnv, callback_obj: JObject, expr: Expr) -> jlong {
        let ldf = self.ldf.clone().filter(expr);

        ldf_to_ptr(env, callback_obj, Ok(ldf))
    }

    pub fn sort(
        &self,
        env: &mut JNIEnv,
        callback_obj: JObject,
        exprs: Vec<Expr>,
        null_last: bool,
        maintain_order: bool,
    ) -> jlong {
        let mut desc: Vec<bool> = Vec::new();
        let mut new_exprs: Vec<Expr> = Vec::new();

        for expr in &exprs {
            match expr {
                Expr::Sort { expr, options } => {
                    desc.push(options.descending);
                    new_exprs.push(*expr.clone());
                },
                e => {
                    desc.push(false) ;
                    new_exprs.push(e.clone());
                },
            }
        }

        let ldf = self
            .ldf
            .clone()
            .sort_by_exprs(new_exprs, desc, null_last, maintain_order);

        ldf_to_ptr(env, callback_obj, Ok(ldf))
    }
}
