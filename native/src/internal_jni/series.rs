#![allow(non_snake_case)]

use jni::objects::ReleaseMode::NoCopyBack;
use jni::objects::{
    JBooleanArray, JDoubleArray, JFloatArray, JIntArray, JLongArray, JObject, JObjectArray, JString,
};
use jni::sys::{jlong, JNI_TRUE};
use jni::JNIEnv;
use jni_fn::jni_fn;
use polars::export::chrono::NaiveDate;
use polars::export::num::ToPrimitive;
use polars::prelude::*;

use crate::internal_jni::utils::{get_string, series_to_ptr};
use crate::j_series::JSeries;

#[jni_fn("org.polars.scala.polars.internal.jni.series$")]
pub fn new_str_series(
    mut env: JNIEnv,
    object: JObject,
    name: JString,
    values: JObjectArray,
) -> jlong {
    let mut data: Vec<String> = Vec::new();
    let num_values = env.get_array_length(&values).unwrap();

    for i in 0..num_values {
        let result = env
            .get_object_array_element(&values, i)
            .map(JString::from)
            .unwrap();
        let val = get_string(&mut env, result, "Unable to get/ convert Expr to UTF8.");

        data.push(val)
    }

    let series_name = get_string(&mut env, name, "Unable to get/ convert value to UTF8.");
    let series = Series::new(series_name.as_str(), data);

    series_to_ptr(&mut env, object, Ok(series))
}

#[jni_fn("org.polars.scala.polars.internal.jni.series$")]
pub fn new_long_series(
    mut env: JNIEnv,
    object: JObject,
    name: JString,
    values: JLongArray,
) -> jlong {
    let arr = unsafe { env.get_array_elements(&values, NoCopyBack).unwrap() };
    let data: Vec<i64> = unsafe {
        std::slice::from_raw_parts(arr.as_ptr(), arr.len())
            .to_vec()
            .iter()
            .map(|p| p.to_i64().unwrap())
            .collect()
    };

    let series_name = get_string(&mut env, name, "Unable to get/ convert value to UTF8.");
    let series = Series::new(series_name.as_str(), data);

    series_to_ptr(&mut env, object, Ok(series))
}

#[jni_fn("org.polars.scala.polars.internal.jni.series$")]
pub fn new_int_series(mut env: JNIEnv, object: JObject, name: JString, values: JIntArray) -> jlong {
    let arr = unsafe { env.get_array_elements(&values, NoCopyBack).unwrap() };
    let data: Vec<i32> = unsafe {
        std::slice::from_raw_parts(arr.as_ptr(), arr.len())
            .to_vec()
            .iter()
            .map(|p| p.to_i32().unwrap())
            .collect()
    };

    let series_name = get_string(&mut env, name, "Unable to get/ convert value to UTF8.");
    let series = Series::new(series_name.as_str(), data);

    series_to_ptr(&mut env, object, Ok(series))
}

#[jni_fn("org.polars.scala.polars.internal.jni.series$")]
pub fn new_float_series(
    mut env: JNIEnv,
    object: JObject,
    name: JString,
    values: JFloatArray,
) -> jlong {
    let arr = unsafe { env.get_array_elements(&values, NoCopyBack).unwrap() };
    let data: Vec<f32> = unsafe {
        std::slice::from_raw_parts(arr.as_ptr(), arr.len())
            .to_vec()
            .iter()
            .map(|p| p.to_f32().unwrap())
            .collect()
    };

    let series_name = get_string(&mut env, name, "Unable to get/ convert value to UTF8.");
    let series = Series::new(series_name.as_str(), data);

    series_to_ptr(&mut env, object, Ok(series))
}

#[jni_fn("org.polars.scala.polars.internal.jni.series$")]
pub fn new_double_series(
    mut env: JNIEnv,
    object: JObject,
    name: JString,
    values: JDoubleArray,
) -> jlong {
    let arr = unsafe { env.get_array_elements(&values, NoCopyBack).unwrap() };
    let data: Vec<f64> = unsafe {
        std::slice::from_raw_parts(arr.as_ptr(), arr.len())
            .to_vec()
            .iter()
            .map(|p| p.to_f64().unwrap())
            .collect()
    };

    let series_name = get_string(&mut env, name, "Unable to get/ convert value to UTF8.");
    let series = Series::new(series_name.as_str(), data);

    series_to_ptr(&mut env, object, Ok(series))
}

#[jni_fn("org.polars.scala.polars.internal.jni.series$")]
pub fn new_boolean_series(
    mut env: JNIEnv,
    object: JObject,
    name: JString,
    values: JBooleanArray,
) -> jlong {
    let arr = unsafe { env.get_array_elements(&values, NoCopyBack).unwrap() };
    let data: Vec<bool> = unsafe {
        std::slice::from_raw_parts(arr.as_ptr(), arr.len())
            .to_vec()
            .iter()
            .map(|p| p.to_u8().unwrap() == JNI_TRUE)
            .collect()
    };

    let series_name = get_string(&mut env, name, "Unable to get/ convert value to UTF8.");
    let series = Series::new(series_name.as_str(), data);

    series_to_ptr(&mut env, object, Ok(series))
}

#[jni_fn("org.polars.scala.polars.internal.jni.series$")]
pub fn new_date_series(
    mut env: JNIEnv,
    object: JObject,
    name: JString,
    values: JObjectArray,
) -> jlong {
    let mut data: Vec<NaiveDate> = Vec::new();
    let num_values = env.get_array_length(&values).unwrap();

    for i in 0..num_values {
        let result = env
            .get_object_array_element(&values, i)
            .map(JString::from)
            .unwrap();
        let val = get_string(&mut env, result, "Unable to get/ convert Expr to UTF8.");
        let date = NaiveDate::parse_from_str(val.as_str(), "%Y-%m-%d")
            .expect(format!("Unable to parse provided value `{}` cannot be parsed to date with format `%Y-%m-%d`", val).as_str());

        data.push(date)
    }

    let series_name = get_string(&mut env, name, "Unable to get/ convert value to UTF8.");
    let series = Series::new(series_name.as_str(), data);

    series_to_ptr(&mut env, object, Ok(series))
}

#[jni_fn("org.polars.scala.polars.internal.jni.series$")]
pub fn show(mut _env: JNIEnv, _object: JObject, ptr: jlong) {
    let j_series = unsafe { &mut *(ptr as *mut JSeries) };
    j_series.show()
}
