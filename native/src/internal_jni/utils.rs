use jni::objects::ReleaseMode::NoCopyBack;
use jni::objects::{
    AutoElementsCritical, JBooleanArray, JDoubleArray, JFloatArray, JIntArray, JLongArray, JObject,
    JObjectArray, JPrimitiveArray, JString, TypeArray,
};
use jni::strings::JNIString;
use jni::sys::{jboolean, jdouble, jfloat, jint, jlong, jobject, jstring, JNI_TRUE};
use jni::JNIEnv;
use num_traits::ToPrimitive;
use polars::io::RowIndex;
use polars::prelude::*;

use crate::j_data_frame::JDataFrame;
use crate::j_expr::JExpr;
use crate::j_lazy_frame::JLazyFrame;
use crate::j_series::JSeries;

// Define a trait for converting Java arrays to Rust Vec
pub trait JavaArrayToVec {
    type Output;
    type InternalType;

    fn get_elements<'local, 'array, 'other_local, 'env>(
        env: &'env mut JNIEnv<'local>,
        array: &'array JPrimitiveArray<'other_local, <Self as JavaArrayToVec>::InternalType>,
    ) -> AutoElementsCritical<'local, 'other_local, 'array, 'env, Self::InternalType>
    where
        <Self as JavaArrayToVec>::InternalType: TypeArray,
    {
        unsafe {
            env.get_array_elements_critical(array, NoCopyBack)
                .expect("Unable to get elements of the array")
        }
    }

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output>;
}

// Implement the trait for each Java array type
impl JavaArrayToVec for JBooleanArray<'_> {
    type Output = bool;
    type InternalType = jboolean;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.to_vec().iter().map(|jb| *jb == JNI_TRUE).collect()
    }
}

impl JavaArrayToVec for JIntArray<'_> {
    type Output = i32;
    type InternalType = jint;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.to_vec().iter().map(|jb| jb.to_i32().unwrap()).collect()
    }
}

impl JavaArrayToVec for JLongArray<'_> {
    type Output = i64;
    type InternalType = jlong;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.to_vec().iter().map(|jb| jb.to_i64().unwrap()).collect()
    }
}

impl JavaArrayToVec for JFloatArray<'_> {
    type Output = f32;
    type InternalType = jfloat;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.to_vec().iter().map(|jb| jb.to_f32().unwrap()).collect()
    }
}

impl JavaArrayToVec for JDoubleArray<'_> {
    type Output = f64;
    type InternalType = jdouble;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.to_vec().iter().map(|jb| jb.to_f64().unwrap()).collect()
    }
}

impl JavaArrayToVec for JObjectArray<'_> {
    type Output = String;
    type InternalType = jobject;
    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let len = env.get_array_length(&array).expect("???");
        let mut result = Vec::with_capacity(len as usize);

        for i in 0..len {
            let obj = env.get_object_array_element(&array, i).expect("???????");
            let s = get_string(env, JString::from(obj), "");
            result.push(s);
        }

        result
    }
}

pub fn get_string(env: &mut JNIEnv, string: JString, error_msg: &str) -> String {
    env.get_string(&string).expect(error_msg).into()
}

pub fn to_jstring<S: Into<JNIString>>(env: &mut JNIEnv, string: S, error_msg: &str) -> jstring {
    env.new_string(string).expect(error_msg).into_raw()
}

pub fn get_file_path(env: &mut JNIEnv, file_path: JString) -> String {
    get_string(env, file_path, "Unable to get/ convert raw path to UTF8.")
}

pub fn get_n_rows(n_rows: jlong) -> Option<usize> {
    if n_rows.is_positive() {
        Some(n_rows as usize)
    } else {
        None
    }
}

pub fn get_row_index(
    env: &mut JNIEnv,
    row_count_col_name: JString,
    row_count_col_offset: jint,
) -> Option<RowIndex> {
    if !row_count_col_name.is_null() {
        Some(RowIndex {
            name: PlSmallStr::from_string(get_string(
                env,
                row_count_col_name,
                "Unable to get/ convert row column name to UTF8.",
            )),
            offset: if row_count_col_offset.is_positive() {
                row_count_col_offset as IdxSize
            } else {
                0
            },
        })
    } else {
        None
    }
}

pub fn ldf_to_ptr(env: &mut JNIEnv, object: JObject, ldf_res: PolarsResult<LazyFrame>) -> jlong {
    let ldf = ldf_res.expect("Cannot create LazyFrame from provided arguments.");

    let global_ref = env.new_global_ref(object).unwrap();
    let j_ldf = JLazyFrame::new(ldf, global_ref);

    Box::into_raw(Box::new(j_ldf)) as jlong
}

pub fn df_to_ptr(env: &mut JNIEnv, object: JObject, df_res: PolarsResult<DataFrame>) -> jlong {
    let df = df_res.expect("Cannot create LazyFrame from provided arguments.");

    let global_ref = env.new_global_ref(object).unwrap();
    let j_ldf = JDataFrame::new(df, global_ref);

    Box::into_raw(Box::new(j_ldf)) as jlong
}

pub fn expr_to_ptr(env: &mut JNIEnv, object: JObject, expr: Expr) -> jlong {
    let global_ref = env.new_global_ref(object).unwrap();
    let j_expr = JExpr::new(expr, global_ref);

    Box::into_raw(Box::new(j_expr)) as jlong
}

pub fn series_to_ptr(env: &mut JNIEnv, object: JObject, series_res: PolarsResult<Series>) -> jlong {
    let series = series_res.expect("Cannot create Series from provided arguments.");

    let global_ref = env.new_global_ref(object).unwrap();
    let j_series = JSeries::new(series, global_ref);

    Box::into_raw(Box::new(j_series)) as jlong
}
