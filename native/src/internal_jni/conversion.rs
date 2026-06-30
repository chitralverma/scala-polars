use anyhow::Context;
use jni::objects::ReleaseMode::NoCopyBack;
use jni::objects::*;
use jni::sys::*;
use jni::{Env, jni_sig, jni_str};
use polars::prelude::*;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::internal_jni::utils::string_to_j_string;

pub trait IntoJava<'a> {
    fn try_into_java(self, env: &mut Env<'a>) -> anyhow::Result<JObject<'a>>;
}

pub struct AnyValueWrapper<'a>(pub AnyValue<'a>);

pub trait JavaArrayToVec {
    type Output;

    fn to_vec(env: &mut Env, array: Self) -> anyhow::Result<Vec<Self::Output>>;
}

/// Reads a primitive array via a JNI critical region. The borrowed region has no null sentinel and
/// only fails on a fatal JVM condition (e.g. OOM), so abort rather than unwind across the boundary.
fn read_primitive_array<'env, 'arr, T, R>(
    env: &mut Env<'env>,
    array: &JPrimitiveArray<'arr, T>,
    map: impl Fn(&T) -> R,
) -> Vec<R>
where
    T: TypeArray,
{
    let elements = unsafe { array.get_elements_critical(env, NoCopyBack) };
    match elements {
        Ok(elements) => elements.iter().map(map).collect(),
        Err(err) => {
            eprintln!("Fatal: failed to get critical elements of the primitive array: {err}");
            std::process::abort();
        },
    }
}

impl JavaArrayToVec for JBooleanArray<'_> {
    type Output = bool;

    fn to_vec(env: &mut Env, array: Self) -> anyhow::Result<Vec<Self::Output>> {
        Ok(read_primitive_array(env, &array, |&jb| jb == JNI_TRUE))
    }
}

impl JavaArrayToVec for JIntArray<'_> {
    type Output = i32;

    fn to_vec(env: &mut Env, array: Self) -> anyhow::Result<Vec<Self::Output>> {
        Ok(read_primitive_array(env, &array, |&v| v))
    }
}

impl JavaArrayToVec for JLongArray<'_> {
    type Output = i64;

    fn to_vec(env: &mut Env, array: Self) -> anyhow::Result<Vec<Self::Output>> {
        Ok(read_primitive_array(env, &array, |&v| v))
    }
}

impl JavaArrayToVec for JFloatArray<'_> {
    type Output = f32;

    fn to_vec(env: &mut Env, array: Self) -> anyhow::Result<Vec<Self::Output>> {
        Ok(read_primitive_array(env, &array, |&v| v))
    }
}

impl JavaArrayToVec for JDoubleArray<'_> {
    type Output = f64;

    fn to_vec(env: &mut Env, array: Self) -> anyhow::Result<Vec<Self::Output>> {
        Ok(read_primitive_array(env, &array, |&v| v))
    }
}

impl JavaArrayToVec for JObjectArray<'_> {
    type Output = jobject;
    fn to_vec(env: &mut Env, array: Self) -> anyhow::Result<Vec<Self::Output>> {
        let len = array
            .len(env)
            .context("Error getting length of the array")?;
        let mut result = Vec::with_capacity(len);

        for i in 0..len {
            let obj = array
                .get_element(env, i)
                .context("Error getting element of the array")?;
            result.push(obj.into_raw());
        }

        Ok(result)
    }
}

impl<'a> IntoJava<'a> for &StructArray {
    fn try_into_java(self, env: &mut Env<'a>) -> anyhow::Result<JObject<'a>> {
        let (fld, _, arrs, _nulls) = self.clone().into_data();

        let iter = fld.iter().zip(arrs).map(|(fld, arr)| {
            // SAFETY:
            // reported data type is correct
            unsafe {
                (
                    fld.name.clone(),
                    Series::_try_from_arrow_unchecked_with_md(
                        fld.name.clone(),
                        vec![arr],
                        fld.dtype(),
                        fld.metadata.as_deref(),
                    ),
                )
            }
        });

        let map = env
            .new_object(jni_str!("java/util/HashMap"), jni_sig!("()V"), &[])
            .context("Failed to initialize map for struct field")?;

        let j_map =
            JMap::cast_local(env, map).context("Failed to initialize map for struct field")?;

        for (name, s) in iter {
            let series = s.context(format!(
                "Failed to retrieve series for struct field `{name}`"
            ))?;

            let name_jstr = string_to_j_string(env, &name)
                .context(format!("Failed to parse value `{name}` as a series name"))?;
            let key_obj = unsafe { JObject::from_raw(env, name_jstr) };
            let key = key_obj.auto();

            // Get first value only as series was sliced beforehand
            let value_obj =
                AnyValueWrapper(series.first().value().as_borrowed()).try_into_java(env)?;
            let value = value_obj.auto();

            j_map
                .put(env, &key, &value)
                .context("Failed to put entry in map for struct field")?;
        }
        Ok(j_map.into())
    }
}

impl<'a> IntoJava<'a> for &[u8] {
    fn try_into_java(self, env: &mut Env<'a>) -> anyhow::Result<JObject<'a>> {
        let byte_array = env
            .new_byte_array(self.len())
            .context("Failed to initialize byte array for binary value")?;

        byte_array
            .set_region(env, 0, unsafe {
                // Safe because `u8` and `jbyte` are compatible in layout
                std::slice::from_raw_parts(self.as_ptr() as *const jbyte, self.len())
            })
            .context("Failed to set data in byte array for binary value")?;

        Ok(JObject::from(byte_array))
    }
}

impl<'a> IntoJava<'a> for Series {
    fn try_into_java(self, env: &mut Env<'a>) -> anyhow::Result<JObject<'a>> {
        let j_list_obj = env
            .new_object(jni_str!("java/util/ArrayList"), jni_sig!("()V"), &[])
            .context("Failed to initialize an array for series values")?;

        let j_list = JList::cast_local(env, j_list_obj)
            .context("Failed to initialize an array for series values")?;

        for any_value in self.iter() {
            let wrapped = AnyValueWrapper(any_value.clone());
            // Auto-delete the per-element local ref so a large series does not
            // overflow the JVM local-reference table before the method returns.
            let element_obj = wrapped.try_into_java(env)?;
            let element = element_obj.auto();
            j_list
                .add(env, &element)
                .context(format!("Failed to set value `{any_value}` from series"))?;
        }
        Ok(j_list.into())
    }
}

impl<'a> IntoJava<'a> for AnyValueWrapper<'_> {
    fn try_into_java(self, env: &mut Env<'a>) -> anyhow::Result<JObject<'a>> {
        let obj = match self.0 {
            AnyValue::Int8(v) => box_integer(env, v as i32)?,
            AnyValue::Int16(v) => box_integer(env, v as i32)?,
            AnyValue::Int32(v) => box_integer(env, v)?,
            AnyValue::UInt8(v) => box_integer(env, v as i32)?,
            AnyValue::UInt16(v) => box_integer(env, v as i32)?,
            AnyValue::UInt32(v) => box_integer(env, v as i32)?,
            AnyValue::Int64(v) => box_long(env, v)?,
            AnyValue::UInt64(v) => box_long(env, v as i64)?,
            AnyValue::Float32(v) => box_float(env, v)?,
            AnyValue::Float64(v) => box_double(env, v)?,
            AnyValue::Date(days) => box_date(env, days as i64)?,
            AnyValue::Time(v) => box_time(env, v)?,
            AnyValue::Datetime(nanos, tu, tz) => {
                box_datetime(env, nanos, tu, tz.as_ref().map(|v| *v))?
            },
            AnyValue::DatetimeOwned(nanos, tu, tz) => box_datetime(env, nanos, tu, tz.as_deref())?,
            AnyValue::List(s) => s.try_into_java(env)?,
            AnyValue::Array(s, _) => s.try_into_java(env)?,
            AnyValue::Binary(slice) => slice.try_into_java(env)?,
            AnyValue::BinaryOwned(v) => v.as_slice().try_into_java(env)?,
            AnyValue::Boolean(v) => box_boolean(env, v)?,
            AnyValue::Decimal(num, _precision, scale) => {
                match Decimal::from_i128_with_scale(num, scale as u32).to_f64() {
                    Some(v) => box_double(env, v)?,
                    None => JObject::null(),
                }
            },
            AnyValue::String(s) => {
                let jstr = string_to_j_string(env, s)
                    .context(format!("Failed to parse string value `{s}` in row"))?;
                unsafe { JObject::from_raw(env, jstr) }
            },
            AnyValue::StringOwned(s) => {
                let jstr = string_to_j_string(env, &s)
                    .context(format!("Failed to parse string value `{s}` in row"))?;
                unsafe { JObject::from_raw(env, jstr) }
            },
            AnyValue::Struct(row_idx, arr, _) => {
                arr.clone().sliced(row_idx, 1).try_into_java(env)?
            },
            _ => JObject::null(),
        };
        Ok(obj)
    }
}

/// Boxes a primitive `value` into its Java wrapper class via the wrapper's static `valueOf`.
pub fn box_primitive<'a, T: Into<JValue<'a>>>(
    env: &mut Env<'a>,
    value: T,
    class: &'static jni::strings::JNIStr,
    method_sig: jni::signature::MethodSignature,
) -> anyhow::Result<JObject<'a>> {
    env.call_static_method(class, jni_str!("valueOf"), method_sig, &[value.into()])
        .and_then(|x| x.l())
        .context("Failed to box primitive value")
}

pub fn box_integer<'a, T: Into<JValue<'a>>>(
    env: &mut Env<'a>,
    value: T,
) -> anyhow::Result<JObject<'a>> {
    box_primitive(
        env,
        value,
        jni_str!("java/lang/Integer"),
        jni_sig!("(I)Ljava/lang/Integer;"),
    )
}

pub fn box_boolean<'a>(env: &mut Env<'a>, value: bool) -> anyhow::Result<JObject<'a>> {
    box_primitive(
        env,
        value,
        jni_str!("java/lang/Boolean"),
        jni_sig!("(Z)Ljava/lang/Boolean;"),
    )
}

pub fn box_long<'a, T: Into<JValue<'a>>>(
    env: &mut Env<'a>,
    value: T,
) -> anyhow::Result<JObject<'a>> {
    box_primitive(
        env,
        value,
        jni_str!("java/lang/Long"),
        jni_sig!("(J)Ljava/lang/Long;"),
    )
}

pub fn box_float<'a, T: Into<JValue<'a>>>(
    env: &mut Env<'a>,
    value: T,
) -> anyhow::Result<JObject<'a>> {
    box_primitive(
        env,
        value,
        jni_str!("java/lang/Float"),
        jni_sig!("(F)Ljava/lang/Float;"),
    )
}

pub fn box_double<'a, T: Into<JValue<'a>>>(
    env: &mut Env<'a>,
    value: T,
) -> anyhow::Result<JObject<'a>> {
    box_primitive(
        env,
        value,
        jni_str!("java/lang/Double"),
        jni_sig!("(D)Ljava/lang/Double;"),
    )
}

pub fn box_date<'a, T: Into<JValue<'a>>>(
    env: &mut Env<'a>,
    value: T,
) -> anyhow::Result<JObject<'a>> {
    env.call_static_method(
        jni_str!("java/time/LocalDate"),
        jni_str!("ofEpochDay"),
        jni_sig!("(J)Ljava/time/LocalDate;"),
        &[value.into()],
    )
    .and_then(|x| x.l())
    .context("Failed to box LocalDate value")
}

pub fn box_time<'a, T: Into<JValue<'a>>>(
    env: &mut Env<'a>,
    value: T,
) -> anyhow::Result<JObject<'a>> {
    env.call_static_method(
        jni_str!("java/time/LocalTime"),
        jni_str!("ofNanoOfDay"),
        jni_sig!("(J)Ljava/time/LocalTime;"),
        &[value.into()],
    )
    .and_then(|x| x.l())
    .context("Failed to box LocalTime value")
}

pub fn box_datetime<'a>(
    env: &mut Env<'a>,
    timestamp: i64,
    time_unit: TimeUnit,
    time_zone: Option<&TimeZone>,
) -> anyhow::Result<JObject<'a>> {
    let nanos = match time_unit {
        TimeUnit::Nanoseconds => timestamp,
        TimeUnit::Microseconds => timestamp * 1_000,
        TimeUnit::Milliseconds => timestamp * 1_000_000,
    };
    let j_instant = env
        .call_static_method(
            jni_str!("java/time/Instant"),
            jni_str!("ofEpochSecond"),
            jni_sig!("(JJ)Ljava/time/Instant;"),
            &[
                JValue::Long(nanos / 1_000_000_000),
                JValue::Long((nanos % 1_000_000_000) as jlong),
            ],
        )
        .and_then(|v| v.l())
        .context(format!("Failed to parse value `{timestamp}` into Instant"))?;

    if let Some(zone) = time_zone {
        let zone_jstr = string_to_j_string(env, zone.as_str())
            .context(format!("Failed to parse value `{zone}` as a string"))?;
        let zone_str_obj = unsafe { JObject::from_raw(env, zone_jstr) };
        let zone_id = env
            .call_static_method(
                jni_str!("java/time/ZoneId"),
                jni_str!("of"),
                jni_sig!("(Ljava/lang/String;)Ljava/time/ZoneId;"),
                &[JValue::Object(&zone_str_obj)],
            )
            .and_then(|v| v.l())
            .context(format!("Failed to parse value `{zone}` into ZoneId"))?;

        env.call_static_method(
            jni_str!("java/time/ZonedDateTime"),
            jni_str!("ofInstant"),
            jni_sig!("(Ljava/time/Instant;Ljava/time/ZoneId;)Ljava/time/ZonedDateTime;"),
            &[JValue::Object(&j_instant), JValue::Object(&zone_id)],
        )
        .and_then(|v| v.l())
        .context(format!("Failed to parse value `{zone}` into ZonedDateTime"))
    } else {
        Ok(j_instant)
    }
}
