use std::fmt::Display;

use anyhow::Context;
use jni::objects::ReleaseMode::NoCopyBack;
use jni::objects::*;
use jni::strings::JNIString;
use jni::sys::*;
use jni::JNIEnv;

use crate::utils::error::ResultExt;

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
            let mut cloned_env = env.unsafe_clone();
            env.get_array_elements_critical(array, NoCopyBack)
                .context("Failed to get elements of the array")
                .unwrap_or_throw(&mut cloned_env)
        }
    }

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output>;
}

impl JavaArrayToVec for JBooleanArray<'_> {
    type Output = bool;
    type InternalType = jboolean;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.iter().map(|&jb| jb == JNI_TRUE).collect()
    }
}

impl JavaArrayToVec for JIntArray<'_> {
    type Output = i32;
    type InternalType = jint;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.iter().copied().collect()
    }
}

impl JavaArrayToVec for JLongArray<'_> {
    type Output = i64;
    type InternalType = jlong;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.iter().copied().collect()
    }
}

impl JavaArrayToVec for JFloatArray<'_> {
    type Output = f32;
    type InternalType = jfloat;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.iter().copied().collect()
    }
}

impl JavaArrayToVec for JDoubleArray<'_> {
    type Output = f64;
    type InternalType = jdouble;

    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let arr = Self::get_elements(env, &array);
        arr.iter().copied().collect()
    }
}

impl JavaArrayToVec for JObjectArray<'_> {
    type Output = jobject;
    type InternalType = jobject;
    fn to_vec(env: &mut JNIEnv, array: Self) -> Vec<Self::Output> {
        let len = env
            .get_array_length(&array)
            .context("Error getting length of the array")
            .unwrap_or_throw(env);
        let mut result = Vec::with_capacity(len as usize);

        for i in 0..len {
            let obj = env
                .get_object_array_element(&array, i)
                .context("Error getting element of the array")
                .unwrap_or_throw(env);
            result.push(obj.into_raw());
        }

        result
    }
}

pub fn string_to_j_string<T, S: Into<JNIString>>(env: &mut JNIEnv, s: S, msg: Option<T>) -> jstring
where
    T: AsRef<str> + Send + Sync + Display + 'static,
{
    if let Some(c) = msg {
        env.new_string(s).context(c)
    } else {
        env.new_string(s)
            .context("Error converting JString to Rust String")
    }
    .unwrap_or_throw(env)
    .as_raw()
}

pub fn j_string_to_string<T>(env: &mut JNIEnv, s: &JString, msg: Option<T>) -> String
where
    T: AsRef<str> + Send + Sync + Display + 'static,
{
    if let Some(c) = msg {
        env.get_string(s).context(c)
    } else {
        env.get_string(s)
            .context("Error converting JString to Rust String")
    }
    .unwrap_or_throw(env)
    .into()
}

pub fn get_n_rows(n_rows: jlong) -> Option<usize> {
    if n_rows.is_positive() {
        Some(n_rows as usize)
    } else {
        None
    }
}

pub fn to_ptr<T: Clone>(v: T) -> jlong {
    Box::into_raw(Box::new(v.clone())) as jlong
}

pub fn find_java_class<'a>(env: &mut JNIEnv<'a>, class: &str) -> JClass<'a> {
    env.find_class(class)
        .context(format!(
            "Error finding Java class for provided value `{class}`"
        ))
        .unwrap_or_throw(env)
}
