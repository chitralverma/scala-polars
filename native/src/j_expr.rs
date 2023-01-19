use crate::internal_jni::utils::expr_to_ptr;
use jni::objects::{GlobalRef, JObject};
use jni::sys::jlong;
use jni::JNIEnv;
use polars::prelude::Expr;

#[derive(Clone)]
pub struct JExpr {
    pub expr: Expr,
    _callback: GlobalRef,
}

impl JExpr {
    pub fn new(expr: Expr, callback: GlobalRef) -> JExpr {
        JExpr {
            expr,
            _callback: callback,
        }
    }

    pub fn not(&self, env: JNIEnv, callback_obj: JObject) -> jlong {
        let res_expr = self.expr.clone().not();

        expr_to_ptr(env, callback_obj, res_expr)
    }

    pub fn and(&self, env: JNIEnv, callback_obj: JObject, right: JExpr) -> jlong {
        let right_expr = right.expr;
        let res_expr = self.expr.clone().and(right_expr);

        expr_to_ptr(env, callback_obj, res_expr)
    }

    pub fn or(&self, env: JNIEnv, callback_obj: JObject, right: JExpr) -> jlong {
        let right_expr = right.expr;
        let res_expr = self.expr.clone().or(right_expr);

        expr_to_ptr(env, callback_obj, res_expr)
    }

    pub fn equal_to(&self, env: JNIEnv, callback_obj: JObject, right: JExpr) -> jlong {
        let right_expr = right.expr;
        let res_expr = self.expr.clone().eq(right_expr);

        expr_to_ptr(env, callback_obj, res_expr)
    }

    pub fn not_equal_to(&self, env: JNIEnv, callback_obj: JObject, right: JExpr) -> jlong {
        let right_expr = right.expr;
        let res_expr = self.expr.clone().neq(right_expr);

        expr_to_ptr(env, callback_obj, res_expr)
    }

    pub fn less_than(&self, env: JNIEnv, callback_obj: JObject, right: JExpr) -> jlong {
        let right_expr = right.expr;
        let res_expr = self.expr.clone().lt(right_expr);

        expr_to_ptr(env, callback_obj, res_expr)
    }

    pub fn less_than_equal_to(&self, env: JNIEnv, callback_obj: JObject, right: JExpr) -> jlong {
        let right_expr = right.expr;
        let res_expr = self.expr.clone().lt_eq(right_expr);

        expr_to_ptr(env, callback_obj, res_expr)
    }

    pub fn greater_than(&self, env: JNIEnv, callback_obj: JObject, right: JExpr) -> jlong {
        let right_expr = right.expr;
        let res_expr = self.expr.clone().gt(right_expr);

        expr_to_ptr(env, callback_obj, res_expr)
    }

    pub fn greater_than_equal_to(&self, env: JNIEnv, callback_obj: JObject, right: JExpr) -> jlong {
        let right_expr = right.expr;
        let res_expr = self.expr.clone().gt_eq(right_expr);

        expr_to_ptr(env, callback_obj, res_expr)
    }

    pub fn is_null(&self, env: JNIEnv, callback_obj: JObject) -> jlong {
        let res_expr = self.expr.clone().is_null();

        expr_to_ptr(env, callback_obj, res_expr)
    }

    pub fn is_not_null(&self, env: JNIEnv, callback_obj: JObject) -> jlong {
        let res_expr = self.expr.clone().is_not_null();

        expr_to_ptr(env, callback_obj, res_expr)
    }

    pub fn is_nan(&self, env: JNIEnv, callback_obj: JObject) -> jlong {
        let res_expr = self.expr.clone().is_nan();

        expr_to_ptr(env, callback_obj, res_expr)
    }

    pub fn is_not_nan(&self, env: JNIEnv, callback_obj: JObject) -> jlong {
        let res_expr = self.expr.clone().is_not_nan();

        expr_to_ptr(env, callback_obj, res_expr)
    }
}
