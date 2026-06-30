use anyhow::Context;
use jni::objects::{JBooleanArray, JLongArray, JMap, JObject, JObjectArray, JString};
use jni::sys::{JNI_TRUE, jboolean, jint, jlong};
use jni::{Env, NativeMethod, native_method};
use polars::prelude::*;
use polars_plan::plans::AExprSorted;

use crate::internal_jni::conversion::JavaArrayToVec;
use crate::internal_jni::handle::{DataFrameHandle, ExprHandle, Handle, LazyFrameHandle};
use crate::internal_jni::macros::decl_free;
use crate::internal_jni::utils::{
    j_object_ref_to_string, j_string_array_to_vec, j_string_to_string,
};
use crate::utils::error::ThrowRuntimeException;

/// Wraps [`native_method!`] with the `lazy_frame$` config common to every entry point in this module.
macro_rules! ldf_method {
    ($($tt:tt)*) => {
        native_method! {
            java_type = "com.github.chitralverma.polars.internal.jni.lazy_frame$",
            error_policy = ThrowRuntimeException,
            type_map = {
                unsafe LazyFrameHandle => long,
                unsafe DataFrameHandle => long,
                unsafe ExprHandle => long,
            },
            $($tt)*
        }
    };
}

const SCHEMA_STRING_METHOD: NativeMethod =
    ldf_method!(extern fn schema_string(ldf: LazyFrameHandle) -> JString, name = "schemaString",);

fn schema_string<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
) -> anyhow::Result<JString<'local>> {
    let mut ldf = ldf.get();
    let schema = ldf
        .collect_schema()
        .map(|op| op.to_arrow(CompatLevel::oldest()))
        .context("Failed to get schema of LazyFrame")?;
    let schema_str = serde_json::to_string(&schema).context("Failed to serialize schema")?;
    JString::from_str(env, schema_str).context("Failed to build schema string")
}

const SELECT_FROM_STRINGS_METHOD: NativeMethod = ldf_method!(extern fn select_from_strings(ldf: LazyFrameHandle, expr_strs: [java.lang.String]) -> LazyFrameHandle, name = "selectFromStrings",);

fn select_from_strings<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    expr_strs: JObjectArray<'local, JString<'local>>,
) -> anyhow::Result<LazyFrameHandle> {
    let exprs: Vec<Expr> = j_string_array_to_vec(
        env,
        &expr_strs,
        "Failed to parse the provided expression value as string",
    )?
    .into_iter()
    .map(col)
    .collect();

    Ok(LazyFrameHandle::alloc(ldf.get().select(exprs)))
}

const SELECT_FROM_EXPRS_METHOD: NativeMethod = ldf_method!(extern fn select_from_exprs(ldf: LazyFrameHandle, inputs: [jlong]) -> LazyFrameHandle, name = "selectFromExprs",);

fn select_from_exprs<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    inputs: JLongArray<'local>,
) -> anyhow::Result<LazyFrameHandle> {
    let exprs: Vec<Expr> = JavaArrayToVec::to_vec(env, inputs)?
        .into_iter()
        .map(|ptr| ExprHandle::from(ptr).get())
        .collect();

    Ok(LazyFrameHandle::alloc(ldf.get().select(exprs)))
}

const FILTER_FROM_EXPRS_METHOD: NativeMethod = ldf_method!(extern fn filter_from_exprs(ldf: LazyFrameHandle, expr: ExprHandle) -> LazyFrameHandle, name = "filterFromExprs",);

fn filter_from_exprs<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    expr: ExprHandle,
) -> anyhow::Result<LazyFrameHandle> {
    Ok(LazyFrameHandle::alloc(ldf.get().filter(expr.get())))
}

const LIMIT_METHOD: NativeMethod =
    ldf_method!(extern fn limit(ldf: LazyFrameHandle, n: jlong) -> LazyFrameHandle,);

fn limit<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    n: jlong,
) -> anyhow::Result<LazyFrameHandle> {
    Ok(LazyFrameHandle::alloc(ldf.get().limit(n as IdxSize)))
}

const TAIL_METHOD: NativeMethod =
    ldf_method!(extern fn tail(ldf: LazyFrameHandle, n: jlong) -> LazyFrameHandle,);

fn tail<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    n: jlong,
) -> anyhow::Result<LazyFrameHandle> {
    Ok(LazyFrameHandle::alloc(ldf.get().tail(n as IdxSize)))
}

const DROP_METHOD: NativeMethod = ldf_method!(extern fn drop(ldf: LazyFrameHandle, col_names: [java.lang.String]) -> LazyFrameHandle,);

fn drop<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    col_names: JObjectArray<'local, JString<'local>>,
) -> anyhow::Result<LazyFrameHandle> {
    let names = j_string_array_to_vec(
        env,
        &col_names,
        "Failed to parse the provided value as column name",
    )?;

    Ok(LazyFrameHandle::alloc(
        ldf.get().drop(by_name(names, false, false)),
    ))
}

const DROP_NULLS_METHOD: NativeMethod = ldf_method!(extern fn drop_nulls(ldf: LazyFrameHandle, subset: [java.lang.String]) -> LazyFrameHandle, name = "dropNulls",);

fn drop_nulls<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    subset: JObjectArray<'local, JString<'local>>,
) -> anyhow::Result<LazyFrameHandle> {
    let cols: Vec<PlSmallStr> = j_string_array_to_vec(
        env,
        &subset,
        "Failed to parse the provided value as column name",
    )?
    .into_iter()
    .map(PlSmallStr::from_string)
    .collect();

    let subset = if cols.is_empty() {
        None
    } else {
        Some(by_name(cols, false, false))
    };

    Ok(LazyFrameHandle::alloc(ldf.get().drop_nulls(subset)))
}

const RENAME_METHOD: NativeMethod =
    ldf_method!(extern fn rename(ldf: LazyFrameHandle, options: java.util.Map) -> LazyFrameHandle,);

fn rename<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    options: JMap<'local>,
) -> anyhow::Result<LazyFrameHandle> {
    let mut map_iterator = options
        .iter(env)
        .context("Failed to get mapping iterator to rename columns")?;

    let mut old_vec: Vec<String> = Vec::new();
    let mut new_vec: Vec<String> = Vec::new();

    while let Some(entry) = map_iterator
        .next(env)
        .context("Failed to read next entry while renaming columns")?
    {
        let key_obj = entry.key(env)?;
        let key_str = j_object_ref_to_string(
            env,
            &key_obj,
            Some("Failed to parse the provided existing column name as string"),
        )?;

        let value_obj = entry.value(env)?;
        let value_str = j_object_ref_to_string(
            env,
            &value_obj,
            Some("Failed to parse the provided new column name as string"),
        )?;

        old_vec.push(key_str);
        new_vec.push(value_str);
    }

    Ok(LazyFrameHandle::alloc(
        ldf.get().rename(old_vec, new_vec, false),
    ))
}

const SORT_FROM_EXPRS_METHOD: NativeMethod = ldf_method!(extern fn sort_from_exprs( ldf: LazyFrameHandle, inputs: [jlong], null_last: [jboolean], maintain_order: jboolean ) -> LazyFrameHandle, name = "sortFromExprs",);

fn sort_from_exprs<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    inputs: JLongArray<'local>,
    null_last: JBooleanArray<'local>,
    maintain_order: jboolean,
) -> anyhow::Result<LazyFrameHandle> {
    let input_exprs: Vec<Expr> = JavaArrayToVec::to_vec(env, inputs)?
        .into_iter()
        .map(|ptr| ExprHandle::from(ptr).get())
        .collect();

    let nulls_last: Vec<bool> = JavaArrayToVec::to_vec(env, null_last)?;

    let (exprs, descending): (Vec<Expr>, Vec<bool>) = input_exprs
        .iter()
        .map(|expr| extract_expr_and_direction(expr, false))
        .unzip();

    let ldf = ldf.get().sort_by_exprs(
        exprs,
        SortMultipleOptions {
            descending,
            nulls_last,
            maintain_order: maintain_order == JNI_TRUE,
            ..Default::default()
        },
    );

    Ok(LazyFrameHandle::alloc(ldf))
}

const TOP_K_FROM_EXPRS_METHOD: NativeMethod = ldf_method!(extern fn top_k_from_exprs( ldf: LazyFrameHandle, k: jint, inputs: [jlong], null_last: [jboolean], maintain_order: jboolean ) -> LazyFrameHandle, name = "topKFromExprs",);

#[allow(clippy::too_many_arguments)]
fn top_k_from_exprs<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    k: jint,
    inputs: JLongArray<'local>,
    null_last: JBooleanArray<'local>,
    maintain_order: jboolean,
) -> anyhow::Result<LazyFrameHandle> {
    let input_exprs: Vec<Expr> = JavaArrayToVec::to_vec(env, inputs)?
        .into_iter()
        .map(|ptr| ExprHandle::from(ptr).get())
        .collect();

    let nulls_last: Vec<bool> = JavaArrayToVec::to_vec(env, null_last)?;

    let (exprs, descending): (Vec<Expr>, Vec<bool>) = input_exprs
        .iter()
        .map(|expr| extract_expr_and_direction(expr, false))
        .unzip();

    let ldf = ldf.get().top_k(
        k as IdxSize,
        exprs,
        SortMultipleOptions {
            descending,
            nulls_last,
            maintain_order: maintain_order == JNI_TRUE,
            ..Default::default()
        },
    );

    Ok(LazyFrameHandle::alloc(ldf))
}

const WITH_COLUMN_METHOD: NativeMethod = ldf_method!(extern fn with_column(ldf: LazyFrameHandle, col_name: java.lang.String, expr: ExprHandle) -> LazyFrameHandle, name = "withColumn",);

fn with_column<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    col_name: JString<'local>,
    expr: ExprHandle,
) -> anyhow::Result<LazyFrameHandle> {
    let name = j_string_to_string(
        env,
        &col_name,
        Some("Failed to parse the provided value as column name"),
    )?;

    Ok(LazyFrameHandle::alloc(
        ldf.get().with_column(expr.get().alias(name)),
    ))
}

const UNIQUE_METHOD: NativeMethod = ldf_method!(extern fn unique( ldf: LazyFrameHandle, subset: [java.lang.String], keep: java.lang.String, maintain_order: jboolean ) -> LazyFrameHandle,);

fn unique<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    subset: JObjectArray<'local, JString<'local>>,
    keep: JString<'local>,
    maintain_order: jboolean,
) -> anyhow::Result<LazyFrameHandle> {
    let cols: Vec<PlSmallStr> = j_string_array_to_vec(
        env,
        &subset,
        "Failed to parse the provided value as column name",
    )?
    .into_iter()
    .map(PlSmallStr::from_string)
    .collect();

    let subset = if cols.is_empty() {
        None
    } else {
        Some(by_name(cols, false, false))
    };

    let keep = match j_string_to_string(
        env,
        &keep,
        Some("Failed to parse the provided value as UniqueKeepStrategy"),
    )?
    .as_str()
    {
        "none" => UniqueKeepStrategy::None,
        "first" => UniqueKeepStrategy::First,
        "last" => UniqueKeepStrategy::Last,
        _ => UniqueKeepStrategy::Any,
    };

    let ldf = ldf.get();
    let unique_ldf = match maintain_order == JNI_TRUE {
        true => ldf.unique_stable(subset, keep),
        false => ldf.unique(subset, keep),
    };

    Ok(LazyFrameHandle::alloc(unique_ldf))
}

const EXPLAIN_METHOD: NativeMethod = ldf_method!(extern fn explain(ldf: LazyFrameHandle, optimized: jboolean, tree_format: jboolean) -> JString,);

fn explain<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    optimized: jboolean,
    tree_format: jboolean,
) -> anyhow::Result<JString<'local>> {
    let ldf = ldf.get();
    let plan_str = if tree_format == JNI_TRUE {
        if optimized == JNI_TRUE {
            ldf.describe_optimized_plan_tree()
        } else {
            ldf.describe_plan_tree()
        }
    } else {
        ldf.explain(optimized == JNI_TRUE)
    }
    .context("Failed to describe plan")?;

    JString::from_str(env, plan_str).context("Failed to build plan string")
}

const SET_SORTED_METHOD: NativeMethod = ldf_method!(extern fn set_sorted( ldf: LazyFrameHandle, column: java.lang.String, descending: jboolean, nulls_last: jboolean ) -> LazyFrameHandle, name = "setSorted",);

fn set_sorted<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    column: JString<'local>,
    descending: jboolean,
    nulls_last: jboolean,
) -> anyhow::Result<LazyFrameHandle> {
    let col_expr = col(j_string_to_string(
        env,
        &column,
        Some("Failed to parse the provided column name as string"),
    )?);

    let sorted_flag = AExprSorted::default()
        .with_desc(Some(descending == JNI_TRUE))
        .with_nulls_last(Some(nulls_last == JNI_TRUE));

    Ok(LazyFrameHandle::alloc(
        ldf.get().with_column(col_expr.set_sorted_flag(sorted_flag)),
    ))
}

const CACHE_METHOD: NativeMethod =
    ldf_method!(extern fn cache(ldf: LazyFrameHandle) -> LazyFrameHandle,);

fn cache<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
) -> anyhow::Result<LazyFrameHandle> {
    Ok(LazyFrameHandle::alloc(ldf.get().cache()))
}

const COLLECT_METHOD: NativeMethod =
    ldf_method!(extern fn collect(ldf: LazyFrameHandle) -> DataFrameHandle,);

fn collect<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
) -> anyhow::Result<DataFrameHandle> {
    let df = ldf
        .get()
        .collect()
        .context("Failed to collect LazyFrame into DataFrame")?;

    Ok(DataFrameHandle::alloc(df))
}

const CONCAT_LAZY_FRAMES_METHOD: NativeMethod = ldf_method!(extern fn concat_lazy_frames(inputs: [jlong], parallel: jboolean, re_chunk: jboolean) -> LazyFrameHandle, name = "concatLazyFrames",);

fn concat_lazy_frames<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    inputs: JLongArray<'local>,
    parallel: jboolean,
    re_chunk: jboolean,
) -> anyhow::Result<LazyFrameHandle> {
    let ldfs: Vec<_> = JavaArrayToVec::to_vec(env, inputs)?
        .into_iter()
        .map(|ptr| LazyFrameHandle::from(ptr).get())
        .collect();

    let concatenated_ldf = concat(
        ldfs,
        UnionArgs {
            parallel: parallel == JNI_TRUE,
            rechunk: re_chunk == JNI_TRUE,
            ..Default::default()
        },
    )
    .context("Failed to concatenate dataframes")?;

    Ok(LazyFrameHandle::alloc(concatenated_ldf))
}

const OPTIMIZATION_TOGGLE_METHOD: NativeMethod = ldf_method!(extern fn optimization_toggle( ldf: LazyFrameHandle, type_coercion: jboolean, predicate_pushdown: jboolean, projection_pushdown: jboolean, simplify_expr: jboolean, slice_pushdown: jboolean, comm_subplan_elim: jboolean, comm_subexpr_elim: jboolean, streaming: jboolean ) -> LazyFrameHandle, name = "optimizationToggle",);

#[allow(clippy::too_many_arguments)]
fn optimization_toggle<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    ldf: LazyFrameHandle,
    type_coercion: jboolean,
    predicate_pushdown: jboolean,
    projection_pushdown: jboolean,
    simplify_expr: jboolean,
    slice_pushdown: jboolean,
    comm_subplan_elim: jboolean,
    comm_subexpr_elim: jboolean,
    streaming: jboolean,
) -> anyhow::Result<LazyFrameHandle> {
    let ldf = ldf
        .get()
        .with_type_coercion(type_coercion == JNI_TRUE)
        .with_predicate_pushdown(predicate_pushdown == JNI_TRUE)
        .with_projection_pushdown(projection_pushdown == JNI_TRUE)
        .with_simplify_expr(simplify_expr == JNI_TRUE)
        .with_slice_pushdown(slice_pushdown == JNI_TRUE)
        .with_comm_subplan_elim(comm_subplan_elim == JNI_TRUE)
        .with_comm_subexpr_elim(comm_subexpr_elim == JNI_TRUE)
        .with_streaming(streaming == JNI_TRUE);

    Ok(LazyFrameHandle::alloc(ldf))
}

decl_free!(
    FREE_METHOD,
    "com.github.chitralverma.polars.internal.jni.lazy_frame$",
    LazyFrameHandle
);

// Helper function to extract non-sort expressions and directions
fn extract_expr_and_direction(expr: &Expr, default_direction: bool) -> (Expr, bool) {
    match expr {
        Expr::Sort { expr, options } => {
            extract_expr_and_direction(&expr.clone(), options.descending)
        },
        _ => (expr.clone(), default_direction),
    }
}

/// All native methods exported by this module.
pub const METHODS: &[NativeMethod] = &[
    SCHEMA_STRING_METHOD,
    SELECT_FROM_STRINGS_METHOD,
    SELECT_FROM_EXPRS_METHOD,
    FILTER_FROM_EXPRS_METHOD,
    LIMIT_METHOD,
    TAIL_METHOD,
    DROP_METHOD,
    DROP_NULLS_METHOD,
    RENAME_METHOD,
    SORT_FROM_EXPRS_METHOD,
    TOP_K_FROM_EXPRS_METHOD,
    WITH_COLUMN_METHOD,
    UNIQUE_METHOD,
    EXPLAIN_METHOD,
    SET_SORTED_METHOD,
    CACHE_METHOD,
    COLLECT_METHOD,
    CONCAT_LAZY_FRAMES_METHOD,
    OPTIMIZATION_TOGGLE_METHOD,
    FREE_METHOD,
];
