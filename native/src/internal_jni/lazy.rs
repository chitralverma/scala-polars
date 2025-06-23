#![allow(non_snake_case)]

use anyhow::Context;
use jni::objects::{JBooleanArray, JClass, JLongArray, JObject, JObjectArray, JString};
use jni::sys::{jboolean, jint, jlong, jstring, JNI_TRUE};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::prelude::*;
use polars_core::series::IsSorted;

use crate::internal_jni::utils::*;
use crate::utils::error::ResultExt;

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn schemaString(mut env: JNIEnv, _: JClass, ldf_ptr: *mut LazyFrame) -> jstring {
    let ldf = &mut *ldf_ptr;
    let schema = ldf
        .collect_schema()
        .map(|op| op.to_arrow(CompatLevel::oldest()))
        .context("Failed to get schema of LazyFrame")
        .unwrap_or_throw(&mut env);

    serde_json::to_string(&schema)
        .map(|schema_string| string_to_j_string(&mut env, schema_string, None::<&str>))
        .context("Failed to serialize schema")
        .unwrap_or_throw(&mut env)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn selectFromStrings(
    mut env: JNIEnv,
    _: JClass,
    ldf_ptr: *mut LazyFrame,
    expr_strs: JObjectArray,
) -> jlong {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(&mut env, expr_strs)
        .into_iter()
        .map(|o| JObject::from_raw(o))
        .map(|o| {
            j_string_to_string(
                &mut env,
                &JString::from(o),
                Some("Failed to parse the provided expression value as string"),
            )
        })
        .map(col)
        .collect();

    let ldf = (*ldf_ptr).clone().select(exprs);
    to_ptr(ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn selectFromExprs(
    mut env: JNIEnv,
    _: JClass,
    ldf_ptr: *mut LazyFrame,
    inputs: JLongArray,
) -> jlong {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(&mut env, inputs)
        .into_iter()
        .map(|ptr| (*(ptr as *mut Expr)).to_owned())
        .collect();

    let ldf = (*ldf_ptr).clone().select(exprs);
    to_ptr(ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn filterFromExprs(
    _: JNIEnv,
    _: JClass,
    ldf_ptr: *mut LazyFrame,
    expr_ptr: *mut Expr,
) -> jlong {
    let ldf = (*ldf_ptr).clone().filter((*expr_ptr).clone());
    to_ptr(ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn sortFromExprs(
    mut env: JNIEnv,
    _: JClass,
    ldf_ptr: *mut LazyFrame,
    inputs: JLongArray,
    nullLast: JBooleanArray,
    maintainOrder: jboolean,
) -> jlong {
    let input_exprs: Vec<Expr> = JavaArrayToVec::to_vec(&mut env, inputs)
        .into_iter()
        .map(|ptr| (*(ptr as *mut Expr)).to_owned())
        .collect();

    let nulls_last: Vec<bool> = JavaArrayToVec::to_vec(&mut env, nullLast);

    // Extract non-sort expressions and their sort directions
    let (exprs, descending): (Vec<Expr>, Vec<bool>) = input_exprs
        .iter()
        .map(|expr| extract_expr_and_direction(expr, false))
        .unzip();

    let ldf = (*ldf_ptr).clone().sort_by_exprs(
        exprs,
        SortMultipleOptions {
            descending,
            nulls_last,
            maintain_order: maintainOrder == JNI_TRUE,
            ..Default::default()
        },
    );

    to_ptr(ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn topKFromExprs(
    mut env: JNIEnv,
    _: JClass,
    ldf_ptr: *mut LazyFrame,
    k: jint,
    inputs: JLongArray,
    nullLast: JBooleanArray,
    maintainOrder: jboolean,
) -> jlong {
    let input_exprs: Vec<Expr> = JavaArrayToVec::to_vec(&mut env, inputs)
        .into_iter()
        .map(|ptr| (*(ptr as *mut Expr)).to_owned())
        .collect();

    let nulls_last: Vec<bool> = JavaArrayToVec::to_vec(&mut env, nullLast);

    // Extract non-sort expressions and their sort directions
    let (exprs, descending): (Vec<Expr>, Vec<bool>) = input_exprs
        .iter()
        .map(|expr| extract_expr_and_direction(expr, false))
        .unzip();

    let ldf = (*ldf_ptr).clone().top_k(
        k as IdxSize,
        exprs,
        SortMultipleOptions {
            descending,
            nulls_last,
            maintain_order: maintainOrder == JNI_TRUE,
            ..Default::default()
        },
    );

    to_ptr(ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn withColumn(
    mut env: JNIEnv,
    _: JClass,
    ldf_ptr: *mut LazyFrame,
    col_name: JString,
    expr_ptr: *mut Expr,
) -> jlong {
    let name = j_string_to_string(
        &mut env,
        &col_name,
        Some("Failed to parse the provided value as column name"),
    );

    let ldf = (*ldf_ptr)
        .clone()
        .with_column((*expr_ptr).clone().alias(name));

    to_ptr(ldf)
}

#[allow(clippy::too_many_arguments)]
#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn optimization_toggle(
    _: JNIEnv,
    _: JClass,
    ldf_ptr: *mut LazyFrame,
    typeCoercion: jboolean,
    predicatePushdown: jboolean,
    projectionPushdown: jboolean,
    simplifyExpr: jboolean,
    slicePushdown: jboolean,
    commSubplanElim: jboolean,
    commSubexprElim: jboolean,
    streaming: jboolean,
) -> jlong {
    let ldf = (*ldf_ptr)
        .clone()
        .with_type_coercion(typeCoercion == JNI_TRUE)
        .with_predicate_pushdown(predicatePushdown == JNI_TRUE)
        .with_projection_pushdown(projectionPushdown == JNI_TRUE)
        .with_simplify_expr(simplifyExpr == JNI_TRUE)
        .with_slice_pushdown(slicePushdown == JNI_TRUE)
        .with_comm_subplan_elim(commSubplanElim == JNI_TRUE)
        .with_comm_subexpr_elim(commSubexprElim == JNI_TRUE)
        .with_streaming(streaming == JNI_TRUE);

    to_ptr(ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn cache(_: JNIEnv, _: JClass, ldf_ptr: *mut LazyFrame) -> jlong {
    let ldf = (*ldf_ptr).clone().cache();
    to_ptr(ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn collect(mut env: JNIEnv, _: JClass, ldf_ptr: *mut LazyFrame) -> jlong {
    let df = (*ldf_ptr)
        .clone()
        .collect()
        .context("Failed to collect LazyFrame into DataFrame")
        .unwrap_or_throw(&mut env);

    to_ptr(df)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn concatLazyFrames(
    mut env: JNIEnv,
    _: JClass,
    inputs: JLongArray,
    parallel: jboolean,
    re_chunk: jboolean,
) -> jlong {
    let ldfs: Vec<_> = JavaArrayToVec::to_vec(&mut env, inputs)
        .into_iter()
        .map(|ptr| (*(ptr as *mut LazyFrame)).to_owned())
        .collect();

    let concatenated_ldf = concat(
        ldfs,
        UnionArgs {
            parallel: parallel == JNI_TRUE,
            rechunk: re_chunk == JNI_TRUE,
            ..Default::default()
        },
    )
    .context("Failed to concatenate dataframes")
    .unwrap_or_throw(&mut env);

    to_ptr(concatenated_ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn limit(_: JNIEnv, _: JClass, ldf_ptr: *mut LazyFrame, n: jlong) -> jlong {
    let ldf = (*ldf_ptr).clone().limit(n as IdxSize);
    to_ptr(ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn tail(_: JNIEnv, _: JClass, ldf_ptr: *mut LazyFrame, n: jlong) -> jlong {
    let ldf = (*ldf_ptr).clone().tail(n as IdxSize);
    to_ptr(ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn drop(
    mut env: JNIEnv,
    _: JClass,
    ldf_ptr: *mut LazyFrame,
    col_names: JObjectArray,
) -> jlong {
    let data: Vec<String> = JavaArrayToVec::to_vec(&mut env, col_names)
        .into_iter()
        .map(|o| JObject::from_raw(o))
        .map(|o| {
            j_string_to_string(
                &mut env,
                &JString::from(o),
                Some("Failed to parse the provided value as column name"),
            )
        })
        .collect();

    let ldf = (*ldf_ptr).clone().drop(data);
    to_ptr(ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn rename(
    mut env: JNIEnv,
    _: JClass,
    ldf_ptr: *mut LazyFrame,
    options: JObject,
) -> jlong {
    let map = env
        .get_map(&options)
        .context("Failed to get mapping to rename columns")
        .unwrap_or_throw(&mut env);

    let mut map_iterator = map
        .iter(&mut env)
        .context("Failed to get mapping to rename columns")
        .unwrap_or_throw(&mut env);

    let mut old_vec: Vec<String> = Vec::new();
    let mut new_vec: Vec<String> = Vec::new();

    while let Ok(Some((new, old))) = map_iterator.next(&mut env) {
        let key_str = j_string_to_string(
            &mut env,
            &JString::from(new),
            Some("Failed to parse the provided existing column name as string"),
        );

        let value_str = j_string_to_string(
            &mut env,
            &JString::from(old),
            Some("Failed to parse the provided new column name as string"),
        );

        old_vec.push(key_str);
        new_vec.push(value_str);
    }

    let ldf = (*ldf_ptr).clone().rename(old_vec, new_vec, false);
    to_ptr(ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn explain(
    mut env: JNIEnv,
    _: JClass,
    ldf_ptr: *mut LazyFrame,
    optimized: jboolean,
    tree_format: jboolean,
) -> jstring {
    let ldf = (*ldf_ptr).clone();
    if tree_format == JNI_TRUE {
        if optimized == JNI_TRUE {
            ldf.describe_optimized_plan_tree()
        } else {
            ldf.describe_plan_tree()
        }
    } else {
        ldf.explain(optimized == JNI_TRUE)
    }
    .map(|plan_str| string_to_j_string(&mut env, plan_str, None::<&str>))
    .context("Failed to describe plan")
    .unwrap_or_throw(&mut env)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn unique(
    mut env: JNIEnv,
    _: JClass,
    ldf_ptr: *mut LazyFrame,
    subset: JObjectArray,
    keep: JString,
    maintainOrder: jboolean,
) -> jlong {
    let cols: Vec<PlSmallStr> = JavaArrayToVec::to_vec(&mut env, subset)
        .into_iter()
        .map(|o| JObject::from_raw(o))
        .map(|o| {
            j_string_to_string(
                &mut env,
                &JString::from(o),
                Some("Failed to parse the provided value as column name"),
            )
            .into()
        })
        .collect();

    let subset: Option<Vec<PlSmallStr>> = if cols.is_empty() { None } else { Some(cols) };

    let keep = match j_string_to_string(
        &mut env,
        &keep,
        Some("Failed to parse the provided value as UniqueKeepStrategy"),
    )
    .as_str()
    {
        "none" => UniqueKeepStrategy::None,
        "first" => UniqueKeepStrategy::First,
        "last" => UniqueKeepStrategy::Last,
        _ => UniqueKeepStrategy::Any,
    };

    let ldf = (*ldf_ptr).clone();
    let unique_ldf = match maintainOrder == JNI_TRUE {
        true => ldf.unique_stable(subset, keep),
        false => ldf.unique(
            subset.map(|vec| vec.into_iter().map(|s| s.into_string()).collect()),
            keep,
        ),
    };

    to_ptr(unique_ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn drop_nulls(
    mut env: JNIEnv,
    _: JClass,
    ldf_ptr: *mut LazyFrame,
    subset: JObjectArray,
) -> jlong {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(&mut env, subset)
        .into_iter()
        .map(|o| JObject::from_raw(o))
        .map(|o| {
            j_string_to_string(
                &mut env,
                &JString::from(o),
                Some("Failed to parse the provided expression value as string"),
            )
        })
        .map(col)
        .collect();

    let sub: Option<Vec<Expr>> = if exprs.is_empty() { None } else { Some(exprs) };

    let ldf = (*ldf_ptr).clone().drop_nulls(sub);
    to_ptr(ldf)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.lazy_frame$")]
pub unsafe fn set_sorted(
    mut env: JNIEnv,
    _: JClass,
    ldf_ptr: *mut LazyFrame,
    mapping: JObject,
) -> jlong {
    let map = env
        .get_map(&mapping)
        .context("Failed to get mapping to rename columns")
        .unwrap_or_throw(&mut env);

    let mut map_iterator = map
        .iter(&mut env)
        .context("Failed to get mapping to rename columns")
        .unwrap_or_throw(&mut env);

    let mut exprs: Vec<Expr> = Vec::new();
    while let Ok(Some((new, is_descending))) = map_iterator.next(&mut env) {
        let col_expr = col(j_string_to_string(
            &mut env,
            &JString::from(new),
            Some("Failed to parse the provided column name as string"),
        ));

        let descending = unsafe { *(is_descending.cast::<jboolean>()) };

        let is_sorted = match descending == JNI_TRUE {
            true => IsSorted::Descending,
            false => IsSorted::Ascending,
        };

        exprs.push(col_expr.set_sorted_flag(is_sorted));
    }

    let ldf = (*ldf_ptr).clone().with_columns(exprs);
    to_ptr(ldf)
}

// Helper function to extract non-sort expressions and directions
fn extract_expr_and_direction(expr: &Expr, default_direction: bool) -> (Expr, bool) {
    match expr {
        Expr::Sort { expr, options } => {
            extract_expr_and_direction(&expr.clone(), options.descending)
        },
        _ => (expr.clone(), default_direction),
    }
}
