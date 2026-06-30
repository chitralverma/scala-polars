use anyhow::Context;
use jni::Env;
use jni::objects::*;
use jni::sys::*;

pub fn string_to_j_string(env: &mut Env, s: impl AsRef<str>) -> anyhow::Result<jstring> {
    Ok(env
        .new_string(s)
        .context("Error converting Rust String to JString")?
        .into_raw())
}

pub fn j_string_to_string<T>(env: &Env, s: &JString, msg: Option<T>) -> anyhow::Result<String>
where
    T: AsRef<str> + Send + Sync + std::fmt::Display + 'static,
{
    let res = s.try_to_string(env);
    if let Some(c) = msg {
        res.context(c)
    } else {
        res.context("Error converting JString to Rust String")
    }
}

/// Reads a `String[]` (typed `JObjectArray<JString>`) into an owned `Vec<String>`.
pub fn j_string_array_to_vec(
    env: &mut Env,
    array: &JObjectArray<JString>,
    msg: &'static str,
) -> anyhow::Result<Vec<String>> {
    let len = array
        .len(env)
        .context("Error getting length of the array")?;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let element = array
            .get_element(env, i)
            .context("Error getting element of the array")?;
        out.push(j_string_to_string(env, &element, Some(msg))?);
    }
    Ok(out)
}

/// Reads a `java.lang.String` referenced by a [`JObject`] into an owned Rust `String`.
pub fn j_object_ref_to_string<T>(
    env: &mut Env,
    o: &JObject,
    msg: Option<T>,
) -> anyhow::Result<String>
where
    T: AsRef<str> + Send + Sync + std::fmt::Display + 'static,
{
    let s = env.as_cast::<JString>(o)?;
    j_string_to_string(env, &s, msg)
}

pub fn get_n_rows(n_rows: jlong) -> Option<usize> {
    if n_rows.is_positive() {
        Some(n_rows as usize)
    } else {
        None
    }
}
