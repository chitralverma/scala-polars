use anyhow::Context;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use jni::objects::{
    JBooleanArray, JDoubleArray, JFloatArray, JIntArray, JLongArray, JObject, JObjectArray, JString,
};
use jni::sys::jlong;
use jni::{Env, NativeMethod, native_method};
use polars::prelude::*;

use crate::internal_jni::conversion::JavaArrayToVec;
use crate::internal_jni::handle::{Handle, SeriesHandle};
use crate::internal_jni::utils::{j_string_array_to_vec, j_string_to_string};
use crate::utils::error::ThrowRuntimeException;

/// Generates a `native_method!` entry point that builds a [`Series`] from a Java primitive array.
///
/// Each invocation produces both the `NativeMethod` constant and the implementation function for a
/// single primitive constructor (`newLongSeries`, `newIntSeries`, ...). The caller passes the
/// constant ident, the function ident, the exact Scala `@native` name, the array element sig type,
/// and the matching Rust array type so the generated symbol stays byte-stable with the Scala side.
macro_rules! impl_new_series {
    ($const_name:ident, $fn_name:ident, $scala_name:literal, [$sig_elem:tt], $java_array:ty) => {
        const $const_name: NativeMethod = native_method! {
            java_type = "com.github.chitralverma.polars.internal.jni.series$",
            error_policy = ThrowRuntimeException,
            type_map = { unsafe SeriesHandle => long },
            extern fn $fn_name(name: java.lang.String, values: [$sig_elem]) -> SeriesHandle,
            name = $scala_name,
        };

        fn $fn_name<'local>(
            env: &mut Env<'local>,
            _this: JObject<'local>,
            name: JString<'local>,
            values: $java_array,
        ) -> anyhow::Result<SeriesHandle> {
            let data = JavaArrayToVec::to_vec(env, values)?;

            let series_name = j_string_to_string(
                env,
                &name,
                Some("Failed to parse the provided value as a series name"),
            )?;

            Ok(SeriesHandle::alloc(Series::new(
                PlSmallStr::from_string(series_name),
                data,
            )))
        }
    };
}

impl_new_series!(
    NEW_LONG_SERIES_METHOD,
    new_long_series,
    "newLongSeries",
    [jlong],
    JLongArray<'local>
);
impl_new_series!(
    NEW_INT_SERIES_METHOD,
    new_int_series,
    "newIntSeries",
    [jint],
    JIntArray<'local>
);
impl_new_series!(
    NEW_FLOAT_SERIES_METHOD,
    new_float_series,
    "newFloatSeries",
    [jfloat],
    JFloatArray<'local>
);
impl_new_series!(
    NEW_DOUBLE_SERIES_METHOD,
    new_double_series,
    "newDoubleSeries",
    [jdouble],
    JDoubleArray<'local>
);
impl_new_series!(
    NEW_BOOLEAN_SERIES_METHOD,
    new_boolean_series,
    "newBooleanSeries",
    [jboolean],
    JBooleanArray<'local>
);

const NEW_STR_SERIES_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.series$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe SeriesHandle => long },
    extern fn new_str_series(name: java.lang.String, values: [java.lang.String]) -> SeriesHandle,
    name = "newStrSeries",
};

fn new_str_series<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    name: JString<'local>,
    values: JObjectArray<'local, JString<'local>>,
) -> anyhow::Result<SeriesHandle> {
    let data = j_string_array_to_vec(
        env,
        &values,
        "Failed to parse the provided value as a series element",
    )?;

    let series_name = j_string_to_string(
        env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    )?;

    Ok(SeriesHandle::alloc(Series::new(
        PlSmallStr::from_string(series_name),
        data,
    )))
}

const NEW_DATE_SERIES_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.series$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe SeriesHandle => long },
    extern fn new_date_series(name: java.lang.String, values: [java.lang.String]) -> SeriesHandle,
    name = "newDateSeries",
};

fn new_date_series<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    name: JString<'local>,
    values: JObjectArray<'local, JString<'local>>,
) -> anyhow::Result<SeriesHandle> {
    let data: Vec<NaiveDate> = j_string_array_to_vec(
        env,
        &values,
        "Failed to parse the provided value as a series element",
    )?
    .into_iter()
    .map(|s| {
        let lit = s.as_str();
        NaiveDate::parse_from_str(lit, "%Y-%m-%d").context(format!(
            "Failed to parse value `{lit}` as date with format `%Y-%m-%d`"
        ))
    })
    .collect::<anyhow::Result<_>>()?;

    let series_name = j_string_to_string(
        env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    )?;

    Ok(SeriesHandle::alloc(Series::new(
        PlSmallStr::from_string(series_name),
        data,
    )))
}

const NEW_TIME_SERIES_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.series$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe SeriesHandle => long },
    extern fn new_time_series(name: java.lang.String, values: [java.lang.String]) -> SeriesHandle,
    name = "newTimeSeries",
};

fn new_time_series<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    name: JString<'local>,
    values: JObjectArray<'local, JString<'local>>,
) -> anyhow::Result<SeriesHandle> {
    let data: Vec<NaiveTime> = j_string_array_to_vec(
        env,
        &values,
        "Failed to parse the provided value as a series element",
    )?
    .into_iter()
    .map(|s| {
        let lit = s.as_str();
        NaiveTime::parse_from_str(lit, "%H:%M:%S%.f").context(format!(
            "Failed to parse value `{lit}` as time with format `%H:%M:%S.f`"
        ))
    })
    .collect::<anyhow::Result<_>>()?;

    let series_name = j_string_to_string(
        env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    )?;

    Ok(SeriesHandle::alloc(Series::new(
        PlSmallStr::from_string(series_name),
        data,
    )))
}

const NEW_DATETIME_SERIES_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.series$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe SeriesHandle => long },
    extern fn new_datetime_series(name: java.lang.String, values: [java.lang.String]) -> SeriesHandle,
    name = "newDatetimeSeries",
};

fn new_datetime_series<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    name: JString<'local>,
    values: JObjectArray<'local, JString<'local>>,
) -> anyhow::Result<SeriesHandle> {
    let data: Vec<NaiveDateTime> = j_string_array_to_vec(
        env,
        &values,
        "Failed to parse the provided value as a series element",
    )?
    .into_iter()
    .map(|s| {
        let lit = s.as_str();
        NaiveDateTime::parse_from_str(lit, "%FT%T%.f").context(format!(
            "Failed to parse value `{lit}` as datetime with format `%FT%T%.f`"
        ))
    })
    .collect::<anyhow::Result<_>>()?;

    let series_name = j_string_to_string(
        env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    )?;

    Ok(SeriesHandle::alloc(Series::new(
        PlSmallStr::from_string(series_name),
        data,
    )))
}

const NEW_LIST_SERIES_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.series$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe SeriesHandle => long },
    extern fn new_list_series(name: java.lang.String, values: [jlong]) -> SeriesHandle,
    name = "newListSeries",
};

fn new_list_series<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    name: JString<'local>,
    values: JLongArray<'local>,
) -> anyhow::Result<SeriesHandle> {
    let data: Vec<Series> = JavaArrayToVec::to_vec(env, values)?
        .into_iter()
        .map(|ptr| SeriesHandle::from(ptr).get())
        .collect();

    let series_name = j_string_to_string(
        env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    )?;

    Ok(SeriesHandle::alloc(Series::new(
        PlSmallStr::from_string(series_name),
        data,
    )))
}

const NEW_STRUCT_SERIES_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.series$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe SeriesHandle => long },
    extern fn new_struct_series(name: java.lang.String, values: [jlong]) -> SeriesHandle,
    name = "newStructSeries",
};

fn new_struct_series<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    name: JString<'local>,
    values: JLongArray<'local>,
) -> anyhow::Result<SeriesHandle> {
    let data: Vec<Series> = JavaArrayToVec::to_vec(env, values)?
        .into_iter()
        .map(|ptr| SeriesHandle::from(ptr).get())
        .collect();

    let series_name = j_string_to_string(
        env,
        &name,
        Some("Failed to parse the provided value as a series name"),
    )?;

    let series = StructChunked::from_series(
        PlSmallStr::from_string(series_name),
        data.len(),
        data.iter(),
    )
    .map(|s| s.into_series())
    .context("Failed to create struct series from provided list of series")?;

    Ok(SeriesHandle::alloc(series))
}

const SHOW_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.series$",
    error_policy = ThrowRuntimeException,
    type_map = { unsafe SeriesHandle => long },
    extern fn show(series_ptr: SeriesHandle),
    name = "show",
};

fn show<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    series_ptr: SeriesHandle,
) -> anyhow::Result<()> {
    let series = series_ptr.get();
    println!("{series:?}");
    Ok(())
}

const FREE_METHOD: NativeMethod = native_method! {
    java_type = "com.github.chitralverma.polars.internal.jni.series$",
    error_policy = ThrowRuntimeException,
    extern fn free(ptr: jlong),
    name = "free",
};

fn free<'local>(_env: &mut Env<'local>, _this: JObject<'local>, ptr: jlong) -> anyhow::Result<()> {
    SeriesHandle::free_raw(ptr);
    Ok(())
}

/// All native methods exported by this module.
pub const METHODS: &[NativeMethod] = &[
    NEW_LONG_SERIES_METHOD,
    NEW_INT_SERIES_METHOD,
    NEW_FLOAT_SERIES_METHOD,
    NEW_DOUBLE_SERIES_METHOD,
    NEW_BOOLEAN_SERIES_METHOD,
    NEW_STR_SERIES_METHOD,
    NEW_DATE_SERIES_METHOD,
    NEW_TIME_SERIES_METHOD,
    NEW_DATETIME_SERIES_METHOD,
    NEW_LIST_SERIES_METHOD,
    NEW_STRUCT_SERIES_METHOD,
    SHOW_METHOD,
    FREE_METHOD,
];
