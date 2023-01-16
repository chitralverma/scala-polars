#![allow(non_snake_case)]

use jni::objects::{JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use polars::prelude::*;

use crate::internal_jni::utils::{expr_to_ptr, get_string};

#[no_mangle]
pub extern "system" fn Java_org_polars_scala_polars_internal_jni_schema_00024_column(
    env: JNIEnv,
    object: JObject,
    col_name: JString,
) -> jlong {
    let name = get_string(env, col_name, "Unable to get/ convert column name to UTF8.");

    let expr = col(name.as_str());
    expr_to_ptr(env, object, expr)
}
