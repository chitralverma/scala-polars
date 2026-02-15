use std::ops::{Add, Div, Mul, Rem, Sub};

use anyhow::Context;
use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::{jint, jlong};
use jni_fn::jni_fn;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use polars::prelude::*;

use crate::internal_jni::utils::{from_ptr, j_string_to_string, to_ptr};
use crate::utils::error::ResultExt;

#[derive(Clone, PartialEq, Eq, Debug, FromPrimitive)]
pub enum BinaryOperator {
    EqualTo = 0,
    NotEqualTo = 1,
    LessThan = 2,
    LessThanEqualTo = 3,
    GreaterThan = 4,
    GreaterThanEqualTo = 5,
    Or = 6,
    And = 7,
    Plus = 8,
    Minus = 9,
    Multiply = 10,
    Divide = 11,
    Modulus = 12,
}

#[derive(Clone, PartialEq, Eq, Debug, FromPrimitive)]
pub enum UnaryOperator {
    NOT = 0,
    IsNull = 1,
    IsNotNull = 2,
    IsNan = 3,
    IsNotNan = 4,
    Between = 5,
    IsIn = 6,
    Like = 7,
    Cast = 8,
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.column_expr$")]
pub fn column(mut env: JNIEnv, _: JClass, value: JString) -> jlong {
    let name = j_string_to_string(
        &mut env,
        &value,
        Some("Failed to parse provided column name as string"),
    );

    let expr = col(name.as_str());
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.column_expr$")]
pub fn sort_column_by_name(mut env: JNIEnv, _: JClass, value: JString, descending: bool) -> jlong {
    let name = j_string_to_string(
        &mut env,
        &value,
        Some("Failed to parse provided column name as string"),
    );

    let expr = Expr::Sort {
        expr: Arc::new(col(name.as_str())),
        options: SortOptions {
            descending,
            ..Default::default()
        },
    };

    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.column_expr$")]
pub fn applyUnary(mut env: JNIEnv, _: JClass, expr_ptr: *mut Expr, operator: jint) -> jlong {
    let l_expr = from_ptr(expr_ptr);

    let expr = UnaryOperator::from_i32(operator)
        .and_then(|option| match option {
            UnaryOperator::NOT => Some(l_expr.not()),
            UnaryOperator::IsNull => Some(l_expr.is_null()),
            UnaryOperator::IsNotNull => Some(l_expr.is_not_null()),
            UnaryOperator::IsNan => Some(l_expr.is_nan()),
            UnaryOperator::IsNotNan => Some(l_expr.is_not_nan()),
            _ => None,
        })
        .context(format!(
            "Failed to parse provided ID `{operator}` as unary operator."
        ))
        .unwrap_or_throw(&mut env);

    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.column_expr$")]
pub fn applyBinary(
    mut env: JNIEnv,
    _: JClass,
    left_ptr: *mut Expr,
    right_ptr: *mut Expr,
    operator: jint,
) -> jlong {
    let l_expr = from_ptr(left_ptr);
    let r_expr = from_ptr(right_ptr);

    let expr = BinaryOperator::from_i32(operator)
        .map(|option| match option {
            BinaryOperator::EqualTo => l_expr.eq(r_expr),
            BinaryOperator::NotEqualTo => l_expr.neq(r_expr),
            BinaryOperator::LessThan => l_expr.lt(r_expr),
            BinaryOperator::LessThanEqualTo => l_expr.lt_eq(r_expr),
            BinaryOperator::GreaterThan => l_expr.gt(r_expr),
            BinaryOperator::GreaterThanEqualTo => l_expr.gt_eq(r_expr),
            BinaryOperator::Or => l_expr.or(r_expr),
            BinaryOperator::And => l_expr.and(r_expr),
            BinaryOperator::Plus => l_expr.add(r_expr),
            BinaryOperator::Minus => l_expr.sub(r_expr),
            BinaryOperator::Multiply => l_expr.mul(r_expr),
            BinaryOperator::Divide => l_expr.div(r_expr),
            BinaryOperator::Modulus => l_expr.rem(r_expr),
        })
        .context(format!(
            "Failed to parse provided ID `{operator}` as binary operator."
        ))
        .unwrap_or_throw(&mut env);

    to_ptr(expr)
}
