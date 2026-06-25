use std::ops::{Add, Div, Mul, Rem, Sub};

use anyhow::Context;
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JObjectArray, JString};
use jni::sys::{jint, jlong};
use jni_fn::jni_fn;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use polars::prelude::*;

use crate::internal_jni::utils::{free_ptr, from_ptr, j_string_to_string, to_ptr};
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
pub fn cast(mut env: JNIEnv, _: JClass, expr_ptr: *mut Expr, dataType: JString) -> jlong {
    let l_expr = from_ptr(expr_ptr);
    let dt_str = j_string_to_string(
        &mut env,
        &dataType,
        Some("Failed to parse provided DataType as string"),
    );

    let dtype: DataType = if dt_str.starts_with('{') {
        serde_json::from_str(&dt_str)
            .context(format!("Failed to deserialize DataType JSON: {dt_str}"))
            .unwrap_or_throw(&mut env)
    } else {
        // Fallback to basic type name parsing
        match dt_str.to_lowercase().as_str() {
            "int8" => DataType::Int8,
            "int16" => DataType::Int16,
            "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "uint8" => DataType::UInt8,
            "uint16" => DataType::UInt16,
            "uint32" => DataType::UInt32,
            "uint64" => DataType::UInt64,
            "float32" => DataType::Float32,
            "float64" => DataType::Float64,
            "boolean" => DataType::Boolean,
            "string" | "utf8" => DataType::String,
            "binary" => DataType::Binary,
            "date" => DataType::Date,
            "null" => DataType::Null,
            _ => {
                // Try serde_json with quoted string
                serde_json::from_str(&format!("\"{dt_str}\""))
                    .context(format!("Failed to parse DataType: {dt_str}"))
                    .unwrap_or_throw(&mut env)
            },
        }
    };

    let expr = l_expr.cast(dtype);
    to_ptr(expr)
}

fn jobject_to_any_value<'local>(
    env: &mut JNIEnv<'local>,
    obj: &JObject<'local>,
) -> AnyValue<'local> {
    if obj.is_null() {
        AnyValue::Null
    } else if env
        .is_instance_of(obj, "java/lang/Integer")
        .unwrap_or(false)
    {
        let val = env
            .call_method(obj, "intValue", "()I", &[])
            .context("Failed to call intValue")
            .unwrap_or_throw(env)
            .i()
            .unwrap_or(0);
        AnyValue::Int32(val)
    } else if env.is_instance_of(obj, "java/lang/Long").unwrap_or(false) {
        let val = env
            .call_method(obj, "longValue", "()J", &[])
            .context("Failed to call longValue")
            .unwrap_or_throw(env)
            .j()
            .unwrap_or(0);
        AnyValue::Int64(val)
    } else if env.is_instance_of(obj, "java/lang/Byte").unwrap_or(false) {
        let val = env
            .call_method(obj, "byteValue", "()B", &[])
            .context("Failed to call byteValue")
            .unwrap_or_throw(env)
            .b()
            .unwrap_or(0);
        AnyValue::Int8(val)
    } else if env.is_instance_of(obj, "java/lang/Short").unwrap_or(false) {
        let val = env
            .call_method(obj, "shortValue", "()S", &[])
            .context("Failed to call shortValue")
            .unwrap_or_throw(env)
            .s()
            .unwrap_or(0);
        AnyValue::Int16(val)
    } else if env.is_instance_of(obj, "java/lang/String").unwrap_or(false) {
        let jstr: &JString = obj.into();
        let s = j_string_to_string(env, jstr, Some("Invalid String"));
        AnyValue::StringOwned(s.into())
    } else if env
        .is_instance_of(obj, "java/lang/Boolean")
        .unwrap_or(false)
    {
        let val = env
            .call_method(obj, "booleanValue", "()Z", &[])
            .context("Failed to call booleanValue")
            .unwrap_or_throw(env)
            .z()
            .unwrap_or(false);
        AnyValue::Boolean(val)
    } else if env.is_instance_of(obj, "java/lang/Double").unwrap_or(false) {
        let val = env
            .call_method(obj, "doubleValue", "()D", &[])
            .context("Failed to call doubleValue")
            .unwrap_or_throw(env)
            .d()
            .unwrap_or(0.0);
        AnyValue::Float64(val)
    } else if env.is_instance_of(obj, "java/lang/Float").unwrap_or(false) {
        let val = env
            .call_method(obj, "floatValue", "()F", &[])
            .context("Failed to call floatValue")
            .unwrap_or_throw(env)
            .f()
            .unwrap_or(0.0);
        AnyValue::Float32(val)
    } else {
        AnyValue::Null
    }
}

fn jobject_to_expr<'local>(env: &mut JNIEnv<'local>, obj: &JObject<'local>) -> Expr {
    if obj.is_null() {
        NULL.lit()
    } else if env
        .is_instance_of(obj, "java/lang/Integer")
        .unwrap_or(false)
    {
        let val = env
            .call_method(obj, "intValue", "()I", &[])
            .context("Failed to call intValue")
            .unwrap_or_throw(env)
            .i()
            .unwrap_or(0);
        lit(val)
    } else if env.is_instance_of(obj, "java/lang/Long").unwrap_or(false) {
        let val = env
            .call_method(obj, "longValue", "()J", &[])
            .context("Failed to call longValue")
            .unwrap_or_throw(env)
            .j()
            .unwrap_or(0);
        lit(val)
    } else if env.is_instance_of(obj, "java/lang/Byte").unwrap_or(false) {
        let val = env
            .call_method(obj, "byteValue", "()B", &[])
            .context("Failed to call byteValue")
            .unwrap_or_throw(env)
            .b()
            .unwrap_or(0);
        lit(val)
    } else if env.is_instance_of(obj, "java/lang/Short").unwrap_or(false) {
        let val = env
            .call_method(obj, "shortValue", "()S", &[])
            .context("Failed to call shortValue")
            .unwrap_or_throw(env)
            .s()
            .unwrap_or(0);
        lit(val)
    } else if env.is_instance_of(obj, "java/lang/String").unwrap_or(false) {
        let jstr: &JString = obj.into();
        let s = j_string_to_string(env, jstr, Some("Invalid String"));
        lit(s)
    } else if env
        .is_instance_of(obj, "java/lang/Boolean")
        .unwrap_or(false)
    {
        let val = env
            .call_method(obj, "booleanValue", "()Z", &[])
            .context("Failed to call booleanValue")
            .unwrap_or_throw(env)
            .z()
            .unwrap_or(false);
        lit(val)
    } else if env.is_instance_of(obj, "java/lang/Double").unwrap_or(false) {
        let val = env
            .call_method(obj, "doubleValue", "()D", &[])
            .context("Failed to call doubleValue")
            .unwrap_or_throw(env)
            .d()
            .unwrap_or(0.0);
        lit(val)
    } else if env.is_instance_of(obj, "java/lang/Float").unwrap_or(false) {
        let val = env
            .call_method(obj, "floatValue", "()F", &[])
            .context("Failed to call floatValue")
            .unwrap_or_throw(env)
            .f()
            .unwrap_or(0.0);
        lit(val)
    } else {
        NULL.lit()
    }
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.column_expr$")]
pub fn isIn(mut env: JNIEnv, _: JClass, expr_ptr: *mut Expr, values: JObjectArray) -> jlong {
    let l_expr = from_ptr(expr_ptr);
    let len = env
        .get_array_length(&values)
        .context("Failed to get array length")
        .unwrap_or_throw(&mut env);
    let mut s_vec = Vec::with_capacity(len as usize);

    for i in 0..len {
        let obj = env
            .get_object_array_element(&values, i)
            .context("Failed to get array element")
            .unwrap_or_throw(&mut env);
        s_vec.push(jobject_to_any_value(&mut env, &obj));
    }

    let s = Series::from_any_values("values".into(), &s_vec, false)
        .context("Failed to build series from any values")
        .unwrap_or_throw(&mut env);
    let expr = l_expr.is_in(lit(s).implode(false), true);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.column_expr$")]
pub fn isBetween<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    expr_ptr: *mut Expr,
    lower: JObject<'local>,
    upper: JObject<'local>,
) -> jlong {
    let l_expr = from_ptr(expr_ptr);

    let l_lit = jobject_to_expr(&mut env, &lower);
    let u_lit = jobject_to_expr(&mut env, &upper);

    let expr = l_expr.is_between(l_lit, u_lit, ClosedInterval::Both);
    to_ptr(expr)
}

fn escape_regex(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len() * 2);
    for c in s.chars() {
        match c {
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' => {
                escaped.push('\\');
                escaped.push(c);
            },
            _ => escaped.push(c),
        }
    }
    escaped
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.column_expr$")]
pub fn like(mut env: JNIEnv, _: JClass, expr_ptr: *mut Expr, pattern: JString) -> jlong {
    let l_expr = from_ptr(expr_ptr);
    let pat = j_string_to_string(
        &mut env,
        &pattern,
        Some("Failed to parse pattern as string"),
    );

    let escaped = escape_regex(&pat);
    let regex_pattern = format!("^{}$", escaped.replace('%', ".*").replace('_', "."));
    let expr = l_expr.str().contains(lit(regex_pattern), true);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.column_expr$")]
pub fn to_uppercase(_: JNIEnv, _: JClass, expr_ptr: *mut Expr) -> jlong {
    let l_expr = from_ptr(expr_ptr);
    let expr = l_expr.str().to_uppercase();
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

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.column_expr$")]
pub fn alias(mut env: JNIEnv, _: JClass, expr_ptr: *mut Expr, name: JString) -> jlong {
    let l_expr = from_ptr(expr_ptr);
    let s_name = j_string_to_string(&mut env, &name, Some("Failed to parse alias name"));
    let expr = l_expr.alias(s_name);
    to_ptr(expr)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.expressions.column_expr$")]
pub fn free(_: JNIEnv, _: JClass, ptr: jlong) {
    free_ptr::<Expr>(ptr);
}
