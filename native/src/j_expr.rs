use jni::objects::GlobalRef;
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
}
