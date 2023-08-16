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

    #[allow(clippy::too_many_arguments)]
    pub fn optimization_toggle(
        &self,
        env: &mut JNIEnv,
        callback_obj: JObject,
        type_coercion: bool,
        predicate_pushdown: bool,
        projection_pushdown: bool,
        simplify_expr: bool,
        slice_pushdown: bool,
        comm_subplan_elim: bool,
        comm_subexpr_elim: bool,
        streaming: bool,
    ) -> jlong {
        let ldf = self.ldf.clone();
        let ldf = ldf
            .with_type_coercion(type_coercion)
            .with_predicate_pushdown(predicate_pushdown)
            .with_projection_pushdown(projection_pushdown)
            .with_simplify_expr(simplify_expr)
            .with_slice_pushdown(slice_pushdown)
            .with_comm_subplan_elim(comm_subplan_elim)
            .with_comm_subexpr_elim(comm_subexpr_elim)
            .with_streaming(streaming);

        ldf_to_ptr(env, callback_obj, Ok(ldf))
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

        fn get_non_sort_expr(expr: &Expr, direction: bool, set: bool) -> (Expr, bool) {
            match expr {
                Expr::Sort { expr, options } => {
                    if set {
                        get_non_sort_expr(&expr.clone(), direction,  true)
                    } else {
                        get_non_sort_expr(&expr.clone(), options.descending , true)
                    }
                }
                e => return (e.clone(), direction),
            }
        }

        for expr in &exprs {
            let (expr, direction) = get_non_sort_expr(expr, false, false);
            desc.push(direction);
            new_exprs.push(expr);
        }

        let ldf = self
            .ldf
            .clone()
            .sort_by_exprs(new_exprs, desc, null_last, maintain_order);

        ldf_to_ptr(env, callback_obj, Ok(ldf))
    }
}
