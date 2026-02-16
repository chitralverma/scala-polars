use std::fmt::Display;

use anyhow::Context;
use jni::JNIEnv;
use jni::objects::*;
use jni::strings::JNIString;
use jni::sys::*;

use crate::utils::error::ResultExt;

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

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn j_object_to_string<T>(env: &mut JNIEnv, o: jobject, msg: Option<T>) -> String
where
    T: AsRef<str> + Send + Sync + Display + 'static,
{
    let jo = unsafe { JObject::from_raw(o) };
    j_string_to_string(env, &JString::from(jo), msg)
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

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn from_ptr<T: Clone>(ptr: *mut T) -> T {
    unsafe { (*ptr).clone() }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn free_ptr<T>(ptr: jlong) {
    if ptr != 0 {
        unsafe {
            let _ = Box::from_raw(ptr as *mut T);
        }
    }
}

pub fn find_java_class<'a>(env: &mut JNIEnv<'a>, class: &str) -> JClass<'a> {
    env.find_class(class)
        .context(format!(
            "Error finding Java class for provided value `{class}`"
        ))
        .unwrap_or_throw(env)
}
