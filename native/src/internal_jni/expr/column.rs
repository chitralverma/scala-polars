use std::ops::{Add, Div, Mul, Neg, Rem, Sub};

use anyhow::Context;
use jni::objects::{JObject, JObjectArray, JString};
use jni::sys::{jdouble, jint, jlong};
use jni::{Env, NativeMethod, jni_sig, jni_str, native_method};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use polars::prelude::*;
use polars_core::series::ops::NullBehavior;

use crate::internal_jni::handle::{ExprHandle, Handle};
use crate::internal_jni::macros::decl_free;
use crate::internal_jni::utils::{j_object_ref_to_string, j_string_to_string};
use crate::utils::error::ThrowRuntimeException;

/// Injects the shared `column_expr$` config into [`native_method!`].
macro_rules! col_method {
    ($($tt:tt)*) => {
        native_method! {
            java_type = "com.github.chitralverma.polars.internal.jni.expressions.column_expr$",
            error_policy = ThrowRuntimeException,
            type_map = { unsafe ExprHandle => long },
            $($tt)*
        }
    };
}

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

/// Generates a [`NativeMethod`] entry point for a unary `Expr -> Expr` transform that takes no
/// extra arguments: it reads the [`ExprHandle`], applies the polars method, and returns a new
/// handle.
///
/// Each invocation emits both the `NativeMethod` constant and the implementation function for a
/// single transform. The caller passes the constant ident, the function ident, the exact Scala
/// `@native` name, and the polars method to invoke, so the generated symbol stays byte-stable with
/// the Scala side.
macro_rules! decl_unary_expr {
    ($(($const_name:ident, $fn_name:ident, $scala_name:literal, $method:ident)),+ $(,)?) => {
        $(
            const $const_name: NativeMethod =
                col_method!(extern fn $fn_name(expr: ExprHandle) -> ExprHandle, name = $scala_name);

            fn $fn_name<'local>(
                _env: &mut Env<'local>,
                _this: JObject<'local>,
                expr: ExprHandle,
            ) -> anyhow::Result<ExprHandle> {
                Ok(ExprHandle::alloc(expr.get().$method()))
            }
        )+
    };
}

decl_unary_expr! {
    (IS_FINITE_METHOD, is_finite, "isFinite", is_finite),
    (IS_INFINITE_METHOD, is_infinite, "isInfinite", is_infinite),
    (DROP_NULLS_METHOD, drop_nulls, "dropNulls", drop_nulls),
    (DROP_NANS_METHOD, drop_nans, "dropNans", drop_nans),
    (REVERSE_METHOD, reverse, "reverse", reverse),
    (IS_UNIQUE_METHOD, is_unique, "isUnique", is_unique),
    (IS_DUPLICATED_METHOD, is_duplicated, "isDuplicated", is_duplicated),
    (IS_FIRST_DISTINCT_METHOD, is_first_distinct, "isFirstDistinct", is_first_distinct),
    (IS_LAST_DISTINCT_METHOD, is_last_distinct, "isLastDistinct", is_last_distinct),
    (UNIQUE_COUNTS_METHOD, unique_counts, "uniqueCounts", unique_counts),
    (SUM_METHOD, sum, "sum", sum),
    (MIN_METHOD, min, "min", min),
    (MAX_METHOD, max, "max", max),
    (MEAN_METHOD, mean, "mean", mean),
    (MEDIAN_METHOD, median, "median", median),
    (PRODUCT_METHOD, product, "product", product),
    (COUNT_METHOD, count, "count", count),
    (LEN_METHOD, len, "len", len),
    (N_UNIQUE_METHOD, n_unique, "nUnique", n_unique),
    (APPROX_N_UNIQUE_METHOD, approx_n_unique, "approxNUnique", approx_n_unique),
    (NULL_COUNT_METHOD, null_count, "nullCount", null_count),
    (FIRST_METHOD, first, "first", first),
    (LAST_METHOD, last, "last", last),
    (ARG_MIN_METHOD, arg_min, "argMin", arg_min),
    (ARG_MAX_METHOD, arg_max, "argMax", arg_max),
    (NEG_METHOD, neg, "neg", neg),
    (FLOOR_METHOD, floor, "floor", floor),
    (CEIL_METHOD, ceil, "ceil", ceil),
    (ABS_METHOD, abs, "abs", abs),
    (SIGN_METHOD, sign, "sign", sign),
    (SQRT_METHOD, sqrt, "sqrt", sqrt),
    (CBRT_METHOD, cbrt, "cbrt", cbrt),
    (EXP_METHOD, exp, "exp", exp),
    (LOG1P_METHOD, log1p, "log1p", log1p),
    (TO_PHYSICAL_METHOD, to_physical, "toPhysical", to_physical),
    (SIN_METHOD, sin, "sin", sin),
    (COS_METHOD, cos, "cos", cos),
    (TAN_METHOD, tan, "tan", tan),
    (COT_METHOD, cot, "cot", cot),
    (ARCSIN_METHOD, arcsin, "arcsin", arcsin),
    (ARCCOS_METHOD, arccos, "arccos", arccos),
    (ARCTAN_METHOD, arctan, "arctan", arctan),
    (SINH_METHOD, sinh, "sinh", sinh),
    (COSH_METHOD, cosh, "cosh", cosh),
    (TANH_METHOD, tanh, "tanh", tanh),
    (ARCSINH_METHOD, arcsinh, "arcsinh", arcsinh),
    (ARCCOSH_METHOD, arccosh, "arccosh", arccosh),
    (ARCTANH_METHOD, arctanh, "arctanh", arctanh),
    (DEGREES_METHOD, degrees, "degrees", degrees),
    (RADIANS_METHOD, radians, "radians", radians),
}

const COLUMN_METHOD: NativeMethod =
    col_method!(extern fn column(value: java.lang.String) -> ExprHandle, name = "column",);

fn column<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    value: JString<'local>,
) -> anyhow::Result<ExprHandle> {
    let name = j_string_to_string(
        env,
        &value,
        Some("Failed to parse provided column name as string"),
    )?;

    Ok(ExprHandle::alloc(col(name.as_str())))
}

const SORT_COLUMN_BY_NAME_METHOD: NativeMethod = col_method!(extern fn sort_column_by_name(value: java.lang.String, descending: bool) -> ExprHandle, name = "sortColumnByName",);

fn sort_column_by_name<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    value: JString<'local>,
    descending: bool,
) -> anyhow::Result<ExprHandle> {
    let name = j_string_to_string(
        env,
        &value,
        Some("Failed to parse provided column name as string"),
    )?;

    let expr = Expr::Sort {
        expr: Arc::new(col(name.as_str())),
        options: SortOptions {
            descending,
            ..Default::default()
        },
    };

    Ok(ExprHandle::alloc(expr))
}

const APPLY_UNARY_METHOD: NativeMethod = col_method!(extern fn apply_unary(expr: ExprHandle, operator: jint) -> ExprHandle, name = "applyUnary",);

fn apply_unary<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    operator: jint,
) -> anyhow::Result<ExprHandle> {
    let l_expr = expr.get();

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
        ))?;

    Ok(ExprHandle::alloc(expr))
}

const CAST_METHOD: NativeMethod = col_method!(extern fn cast(expr: ExprHandle, data_type: java.lang.String) -> ExprHandle, name = "cast",);

fn cast<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    data_type: JString<'local>,
) -> anyhow::Result<ExprHandle> {
    let l_expr = expr.get();
    let dt_str = j_string_to_string(
        env,
        &data_type,
        Some("Failed to parse provided DataType as string"),
    )?;

    let dtype: DataType = if dt_str.starts_with('{') {
        serde_json::from_str(&dt_str)
            .context(format!("Failed to deserialize DataType JSON: {dt_str}"))?
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
                    .context(format!("Failed to parse DataType: {dt_str}"))?
            },
        }
    };

    Ok(ExprHandle::alloc(l_expr.cast(dtype)))
}

/// Decodes a boxed Java number/boolean into an [`AnyValue`] by checking its wrapper
/// type and unboxing via the matching accessor. Tries each `($class, $method, $sig,
/// $accessor, $default, $variant)` in order; falls through to the trailing logic.
macro_rules! unbox_java_value {
    ($env:expr, $obj:expr, $( ($class:literal, $method:literal, $sig:literal, $acc:ident, $default:expr, $variant:path) ),+ $(,)?) => {
        $(
            if $env.is_instance_of($obj, jni_str!($class)).unwrap_or(false) {
                let val = $env
                    .call_method($obj, jni_str!($method), jni_sig!($sig), &[])
                    .context(concat!("Failed to call ", $method))?
                    .$acc()
                    .unwrap_or($default);
                return Ok($variant(val));
            }
        )+
    };
}

fn jobject_to_any_value<'local>(
    env: &mut Env<'local>,
    obj: &JObject<'local>,
) -> anyhow::Result<AnyValue<'local>> {
    if obj.is_null() {
        return Ok(AnyValue::Null);
    }

    unbox_java_value!(
        env,
        obj,
        (
            "java/lang/Integer",
            "intValue",
            "()I",
            i,
            0,
            AnyValue::Int32
        ),
        ("java/lang/Long", "longValue", "()J", j, 0, AnyValue::Int64),
        ("java/lang/Byte", "byteValue", "()B", b, 0, AnyValue::Int8),
        (
            "java/lang/Short",
            "shortValue",
            "()S",
            s,
            0,
            AnyValue::Int16
        ),
        (
            "java/lang/Boolean",
            "booleanValue",
            "()Z",
            z,
            false,
            AnyValue::Boolean
        ),
        (
            "java/lang/Double",
            "doubleValue",
            "()D",
            d,
            0.0,
            AnyValue::Float64
        ),
        (
            "java/lang/Float",
            "floatValue",
            "()F",
            f,
            0.0,
            AnyValue::Float32
        ),
    );

    if env
        .is_instance_of(obj, jni_str!("java/lang/String"))
        .unwrap_or(false)
    {
        let s = j_object_ref_to_string(env, obj, Some("Invalid String"))?;
        return Ok(AnyValue::StringOwned(s.into()));
    }

    Ok(AnyValue::Null)
}

fn jobject_to_expr<'local>(env: &mut Env<'local>, obj: &JObject<'local>) -> anyhow::Result<Expr> {
    // Reuse the single boxed-value decoder, then lift the AnyValue into a literal
    // Expr. This keeps the Java type-dispatch ladder in one place.
    let expr = match jobject_to_any_value(env, obj)? {
        AnyValue::Int8(v) => lit(v),
        AnyValue::Int16(v) => lit(v),
        AnyValue::Int32(v) => lit(v),
        AnyValue::Int64(v) => lit(v),
        AnyValue::Float32(v) => lit(v),
        AnyValue::Float64(v) => lit(v),
        AnyValue::Boolean(v) => lit(v),
        AnyValue::StringOwned(s) => lit(s.to_string()),
        _ => NULL.lit(),
    };
    Ok(expr)
}

const IS_IN_METHOD: NativeMethod = col_method!(extern fn is_in(expr: ExprHandle, values: [java.lang.Object]) -> ExprHandle, name = "isIn",);

fn is_in<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    values: JObjectArray<'local, JObject<'local>>,
) -> anyhow::Result<ExprHandle> {
    let l_expr = expr.get();
    let len = values.len(env).context("Failed to get array length")?;
    let mut s_vec = Vec::with_capacity(len);

    for i in 0..len {
        let obj = values
            .get_element(env, i)
            .context("Failed to get array element")?;
        s_vec.push(jobject_to_any_value(env, &obj)?);
    }

    let s = Series::from_any_values("values".into(), &s_vec, false)
        .context("Failed to build series from any values")?;
    let expr = l_expr.is_in(lit(s).implode(false), true);
    Ok(ExprHandle::alloc(expr))
}

const IS_BETWEEN_METHOD: NativeMethod = col_method!(extern fn is_between(expr: ExprHandle, lower: java.lang.Object, upper: java.lang.Object) -> ExprHandle, name = "isBetween",);

fn is_between<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    lower: JObject<'local>,
    upper: JObject<'local>,
) -> anyhow::Result<ExprHandle> {
    let l_expr = expr.get();

    let l_lit = jobject_to_expr(env, &lower)?;
    let u_lit = jobject_to_expr(env, &upper)?;

    let expr = l_expr.is_between(l_lit, u_lit, ClosedInterval::Both);
    Ok(ExprHandle::alloc(expr))
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

const LIKE_METHOD: NativeMethod = col_method!(extern fn like(expr: ExprHandle, pattern: java.lang.String) -> ExprHandle, name = "like",);

fn like<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    pattern: JString<'local>,
) -> anyhow::Result<ExprHandle> {
    let l_expr = expr.get();
    let pat = j_string_to_string(env, &pattern, Some("Failed to parse pattern as string"))?;

    let escaped = escape_regex(&pat);
    let regex_pattern = format!("^{}$", escaped.replace('%', ".*").replace('_', "."));
    let expr = l_expr.str().contains(lit(regex_pattern), true);
    Ok(ExprHandle::alloc(expr))
}

const TO_UPPERCASE_METHOD: NativeMethod =
    col_method!(extern fn to_uppercase(expr: ExprHandle) -> ExprHandle, name = "toUppercase",);

fn to_uppercase<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().str().to_uppercase()))
}

const APPLY_BINARY_METHOD: NativeMethod = col_method!(extern fn apply_binary(left: ExprHandle, right: ExprHandle, operator: jint) -> ExprHandle, name = "applyBinary",);

fn apply_binary<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    left: ExprHandle,
    right: ExprHandle,
    operator: jint,
) -> anyhow::Result<ExprHandle> {
    let l_expr = left.get();
    let r_expr = right.get();

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
        ))?;

    Ok(ExprHandle::alloc(expr))
}

const ALIAS_METHOD: NativeMethod = col_method!(extern fn alias(expr: ExprHandle, name: java.lang.String) -> ExprHandle, name = "alias",);

fn alias<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    name: JString<'local>,
) -> anyhow::Result<ExprHandle> {
    let s_name = j_string_to_string(env, &name, Some("Failed to parse alias name"))?;
    Ok(ExprHandle::alloc(expr.get().alias(s_name)))
}

const IS_EMPTY_METHOD: NativeMethod =
    col_method!(extern fn is_empty(expr: ExprHandle) -> ExprHandle, name = "isEmpty",);

fn is_empty<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().is_empty(false)))
}

const SLICE_METHOD: NativeMethod = col_method!(extern fn slice(expr: ExprHandle, offset: jlong, length: jlong) -> ExprHandle, name = "slice",);

fn slice<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    offset: jlong,
    length: jlong,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(
        expr.get().slice(lit(offset), lit(length)),
    ))
}

const SHIFT_METHOD: NativeMethod =
    col_method!(extern fn shift(expr: ExprHandle, periods: jlong) -> ExprHandle, name = "shift",);

fn shift<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    periods: jlong,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().shift(lit(periods))))
}

const GATHER_EVERY_METHOD: NativeMethod = col_method!(extern fn gather_every(expr: ExprHandle, n: jlong, offset: jlong) -> ExprHandle, name = "gatherEvery",);

fn gather_every<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    n: jlong,
    offset: jlong,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(
        expr.get().gather_every(n as usize, offset as usize),
    ))
}

const UNIQUE_METHOD: NativeMethod = col_method!(extern fn unique(expr: ExprHandle, maintain_order: bool) -> ExprHandle, name = "unique",);

fn unique<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    maintain_order: bool,
) -> anyhow::Result<ExprHandle> {
    let l_expr = expr.get();
    let expr = if maintain_order {
        l_expr.unique_stable()
    } else {
        l_expr.unique()
    };
    Ok(ExprHandle::alloc(expr))
}

const MODE_METHOD: NativeMethod =
    col_method!(extern fn mode(expr: ExprHandle) -> ExprHandle, name = "mode",);

fn mode<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().mode(false)))
}

const STD_METHOD: NativeMethod =
    col_method!(extern fn std(expr: ExprHandle, ddof: jint) -> ExprHandle, name = "std",);

fn std<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    ddof: jint,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().std(ddof as u8)))
}

const VAR_METHOD: NativeMethod =
    col_method!(extern fn var(expr: ExprHandle, ddof: jint) -> ExprHandle, name = "var",);

fn var<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    ddof: jint,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().var(ddof as u8)))
}

const QUANTILE_METHOD: NativeMethod = col_method!(extern fn quantile(expr: ExprHandle, q: jdouble, method: java.lang.String) -> ExprHandle, name = "quantile",);

fn quantile<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    q: jdouble,
    method: JString<'local>,
) -> anyhow::Result<ExprHandle> {
    let l_expr = expr.get();
    let s_method = j_string_to_string(env, &method, Some("Failed to parse quantile method"))?;
    let q_method = match s_method.to_lowercase().as_str() {
        "nearest" => QuantileMethod::Nearest,
        "lower" => QuantileMethod::Lower,
        "higher" => QuantileMethod::Higher,
        "midpoint" => QuantileMethod::Midpoint,
        "linear" => QuantileMethod::Linear,
        "equiprobable" => QuantileMethod::Equiprobable,
        _ => QuantileMethod::Nearest,
    };
    Ok(ExprHandle::alloc(l_expr.quantile(lit(q), q_method)))
}

const ARG_SORT_METHOD: NativeMethod = col_method!(extern fn arg_sort(expr: ExprHandle, descending: bool, nulls_last: bool) -> ExprHandle, name = "argSort",);

fn arg_sort<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    descending: bool,
    nulls_last: bool,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(
        expr.get().arg_sort(descending, nulls_last),
    ))
}

const SKEW_METHOD: NativeMethod =
    col_method!(extern fn skew(expr: ExprHandle, bias: bool) -> ExprHandle, name = "skew",);

fn skew<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    bias: bool,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().skew(bias)))
}

const KURTOSIS_METHOD: NativeMethod = col_method!(extern fn kurtosis(expr: ExprHandle, fisher: bool, bias: bool) -> ExprHandle, name = "kurtosis",);

fn kurtosis<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    fisher: bool,
    bias: bool,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().kurtosis(fisher, bias)))
}

const ANY_METHOD: NativeMethod =
    col_method!(extern fn any(expr: ExprHandle, ignore_nulls: bool) -> ExprHandle, name = "any",);

fn any<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    ignore_nulls: bool,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().any(ignore_nulls)))
}

const ALL_METHOD: NativeMethod =
    col_method!(extern fn all(expr: ExprHandle, ignore_nulls: bool) -> ExprHandle, name = "all",);

fn all<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    ignore_nulls: bool,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().all(ignore_nulls)))
}

const CUM_SUM_METHOD: NativeMethod =
    col_method!(extern fn cum_sum(expr: ExprHandle, reverse: bool) -> ExprHandle, name = "cumSum",);

fn cum_sum<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    reverse: bool,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().cum_sum(reverse)))
}

const POW_METHOD: NativeMethod =
    col_method!(extern fn pow(expr: ExprHandle, exponent: jdouble) -> ExprHandle, name = "pow",);

fn pow<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    exponent: jdouble,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().pow(lit(exponent))))
}

const FLOOR_DIV_METHOD: NativeMethod = col_method!(extern fn floor_div(expr: ExprHandle, other: ExprHandle) -> ExprHandle, name = "floorDiv",);

fn floor_div<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    other: ExprHandle,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().floor_div(other.get())))
}

const ROUND_METHOD: NativeMethod =
    col_method!(extern fn round(expr: ExprHandle, decimals: jint) -> ExprHandle, name = "round",);

fn round<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    decimals: jint,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(
        expr.get().round(decimals as u32, RoundMode::HalfToEven),
    ))
}

const ROUND_SIG_FIGS_METHOD: NativeMethod = col_method!(extern fn round_sig_figs(expr: ExprHandle, digits: jint) -> ExprHandle, name = "roundSigFigs",);

fn round_sig_figs<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    digits: jint,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().round_sig_figs(digits)))
}

const TRUNCATE_METHOD: NativeMethod = col_method!(extern fn truncate(expr: ExprHandle, decimals: jint) -> ExprHandle, name = "truncate",);

fn truncate<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    decimals: jint,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().truncate(decimals as u32)))
}

const CLIP_METHOD: NativeMethod = col_method!(extern fn clip(expr: ExprHandle, lower: ExprHandle, upper: ExprHandle) -> ExprHandle, name = "clip",);

fn clip<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    lower: ExprHandle,
    upper: ExprHandle,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().clip(lower.get(), upper.get())))
}

const CLIP_MIN_METHOD: NativeMethod = col_method!(extern fn clip_min(expr: ExprHandle, lower: ExprHandle) -> ExprHandle, name = "clipMin",);

fn clip_min<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    lower: ExprHandle,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().clip_min(lower.get())))
}

const CLIP_MAX_METHOD: NativeMethod = col_method!(extern fn clip_max(expr: ExprHandle, upper: ExprHandle) -> ExprHandle, name = "clipMax",);

fn clip_max<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    upper: ExprHandle,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().clip_max(upper.get())))
}

const LOG_METHOD: NativeMethod =
    col_method!(extern fn log(expr: ExprHandle, base: jdouble) -> ExprHandle, name = "log",);

fn log<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    base: jdouble,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().log(lit(base))))
}

const DIFF_METHOD: NativeMethod = col_method!(extern fn diff(expr: ExprHandle, n: jlong, null_behavior: java.lang.String) -> ExprHandle, name = "diff",);

fn diff<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    n: jlong,
    null_behavior: JString<'local>,
) -> anyhow::Result<ExprHandle> {
    let behavior = j_string_to_string(
        env,
        &null_behavior,
        Some("Failed to parse null behavior as string"),
    )?;
    let nb = match behavior.to_lowercase().as_str() {
        "drop" => NullBehavior::Drop,
        _ => NullBehavior::Ignore,
    };
    Ok(ExprHandle::alloc(expr.get().diff(lit(n), nb)))
}

const PCT_CHANGE_METHOD: NativeMethod = col_method!(extern fn pct_change(expr: ExprHandle, n: jlong) -> ExprHandle, name = "pctChange",);

fn pct_change<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    n: jlong,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(expr.get().pct_change(lit(n))))
}

const REINTERPRET_METHOD: NativeMethod = col_method!(extern fn reinterpret(expr: ExprHandle, signed: bool) -> ExprHandle, name = "reinterpret",);

fn reinterpret<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    expr: ExprHandle,
    signed: bool,
) -> anyhow::Result<ExprHandle> {
    Ok(ExprHandle::alloc(
        expr.get().reinterpret(Some(signed), None),
    ))
}

decl_free!(
    FREE_METHOD,
    "com.github.chitralverma.polars.internal.jni.expressions.column_expr$",
    ExprHandle
);

pub const METHODS: &[NativeMethod] = &[
    IS_FINITE_METHOD,
    IS_INFINITE_METHOD,
    DROP_NULLS_METHOD,
    DROP_NANS_METHOD,
    REVERSE_METHOD,
    IS_UNIQUE_METHOD,
    IS_DUPLICATED_METHOD,
    IS_FIRST_DISTINCT_METHOD,
    IS_LAST_DISTINCT_METHOD,
    UNIQUE_COUNTS_METHOD,
    SUM_METHOD,
    MIN_METHOD,
    MAX_METHOD,
    MEAN_METHOD,
    MEDIAN_METHOD,
    PRODUCT_METHOD,
    COUNT_METHOD,
    LEN_METHOD,
    N_UNIQUE_METHOD,
    APPROX_N_UNIQUE_METHOD,
    NULL_COUNT_METHOD,
    FIRST_METHOD,
    LAST_METHOD,
    ARG_MIN_METHOD,
    ARG_MAX_METHOD,
    NEG_METHOD,
    FLOOR_METHOD,
    CEIL_METHOD,
    ABS_METHOD,
    SIGN_METHOD,
    SQRT_METHOD,
    CBRT_METHOD,
    EXP_METHOD,
    LOG1P_METHOD,
    TO_PHYSICAL_METHOD,
    SIN_METHOD,
    COS_METHOD,
    TAN_METHOD,
    COT_METHOD,
    ARCSIN_METHOD,
    ARCCOS_METHOD,
    ARCTAN_METHOD,
    SINH_METHOD,
    COSH_METHOD,
    TANH_METHOD,
    ARCSINH_METHOD,
    ARCCOSH_METHOD,
    ARCTANH_METHOD,
    DEGREES_METHOD,
    RADIANS_METHOD,
    COLUMN_METHOD,
    SORT_COLUMN_BY_NAME_METHOD,
    APPLY_UNARY_METHOD,
    CAST_METHOD,
    IS_IN_METHOD,
    IS_BETWEEN_METHOD,
    LIKE_METHOD,
    TO_UPPERCASE_METHOD,
    APPLY_BINARY_METHOD,
    ALIAS_METHOD,
    IS_EMPTY_METHOD,
    SLICE_METHOD,
    SHIFT_METHOD,
    GATHER_EVERY_METHOD,
    UNIQUE_METHOD,
    MODE_METHOD,
    STD_METHOD,
    VAR_METHOD,
    QUANTILE_METHOD,
    ARG_SORT_METHOD,
    SKEW_METHOD,
    KURTOSIS_METHOD,
    ANY_METHOD,
    ALL_METHOD,
    CUM_SUM_METHOD,
    POW_METHOD,
    FLOOR_DIV_METHOD,
    ROUND_METHOD,
    ROUND_SIG_FIGS_METHOD,
    TRUNCATE_METHOD,
    CLIP_METHOD,
    CLIP_MIN_METHOD,
    CLIP_MAX_METHOD,
    LOG_METHOD,
    DIFF_METHOD,
    PCT_CHANGE_METHOD,
    REINTERPRET_METHOD,
    FREE_METHOD,
];
