#![allow(non_snake_case)]

use std::ops::{Add, Div, Mul, Rem, Sub};

use jni::objects::{JObject, JString};
use jni::sys::jint;
use jni::sys::jlong;
use jni::JNIEnv;
use jni_fn::jni_fn;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use polars::prelude::*;

use crate::internal_jni::utils::{expr_to_ptr, get_string};
use crate::j_expr::JExpr;

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

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn column(env: JNIEnv, object: JObject, col_name: JString) -> jlong {
    let name = get_string(env, col_name, "Unable to get/ convert column name to UTF8.");

    let expr = col(name.as_str());
    expr_to_ptr(env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn applyUnary(env: JNIEnv, object: JObject, ptr: jlong, operator: jint) -> jlong {
    let left_expr = unsafe { &mut *(ptr as *mut JExpr) };

    let option = UnaryOperator::from_i32(operator)
        .unwrap_or_else(|| panic!("Unsupported unary operator with ID `{operator}` provided."));

    let l_copy = left_expr.expr.clone();

    let expr_opt = match option {
        UnaryOperator::NOT => Some(l_copy.not()),
        UnaryOperator::IsNull => Some(l_copy.is_null()),
        UnaryOperator::IsNotNull => Some(l_copy.is_not_null()),
        UnaryOperator::IsNan => Some(l_copy.is_nan()),
        UnaryOperator::IsNotNan => Some(l_copy.is_not_nan()),
        _ => None,
    };

    let expr = expr_opt
        .unwrap_or_else(|| panic!("Unsupported unary operator with ID `{operator}` provided."));

    expr_to_ptr(env, object, expr)
}

#[jni_fn("org.polars.scala.polars.internal.jni.expressions.column_expr$")]
pub fn applyBinary(
    env: JNIEnv,
    object: JObject,
    left_ptr: jlong,
    right_ptr: jlong,
    operator: jint,
) -> jlong {
    let left_expr = unsafe { &mut *(left_ptr as *mut JExpr) };
    let right_expr = unsafe { &mut *(right_ptr as *mut JExpr) };

    let option = BinaryOperator::from_i32(operator)
        .unwrap_or_else(|| panic!("Unsupported binary operator with ID `{operator}` provided."));

    let l_copy = left_expr.expr.clone();
    let r_copy = right_expr.expr.clone();

    let expr = match option {
        BinaryOperator::EqualTo => l_copy.eq(r_copy),
        BinaryOperator::NotEqualTo => l_copy.neq(r_copy),
        BinaryOperator::LessThan => l_copy.lt(r_copy),
        BinaryOperator::LessThanEqualTo => l_copy.lt_eq(r_copy),
        BinaryOperator::GreaterThan => l_copy.gt(r_copy),
        BinaryOperator::GreaterThanEqualTo => l_copy.gt_eq(r_copy),
        BinaryOperator::Or => l_copy.or(r_copy),
        BinaryOperator::And => l_copy.and(r_copy),
        BinaryOperator::Plus => l_copy.add(r_copy),
        BinaryOperator::Minus => l_copy.sub(r_copy),
        BinaryOperator::Multiply => l_copy.mul(r_copy),
        BinaryOperator::Divide => l_copy.div(r_copy),
        BinaryOperator::Modulus => l_copy.rem(r_copy),
    };

    expr_to_ptr(env, object, expr)
}
