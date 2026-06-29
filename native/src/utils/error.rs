use anyhow::Error;
use jni::{JNIEnv, sys};
use polars::prelude::{DataFrame, DataType, Expr, LazyFrame, PlHashMap, Series};

fn format_nested_error(error: &Error) -> String {
    let mut formatted = String::new();

    for (i, cause) in error.chain().enumerate() {
        if i == 0 {
            formatted.push_str(&format!("{cause}\n",));
        } else {
            formatted.push_str(&format!("  Caused by: {cause}\n",));
        }
    }

    formatted.trim_end().to_string()
}

/// Throws `err` as a Java `RuntimeException`, unless an exception is already pending
/// (re-throwing over one fails and hides the original).
pub fn throw_java_exception(env: &mut JNIEnv, err: Error) {
    if env.exception_check().unwrap_or(false) {
        return;
    }

    // Resolved directly (not via find_java_class) to avoid recursion.
    let throw_res = env
        .find_class("java/lang/RuntimeException")
        .and_then(|class| env.throw_new(class, format_nested_error(&err)));
    let _ = throw_res;
}

/// Trait to unwrap `Result` or throw an exception.
pub trait ResultExt<T> {
    fn unwrap_or_throw(self, env: &mut JNIEnv) -> T;
}

// Throws the error, then returns the default sentinel for the JNI boundary.
macro_rules! impl_result_ext {
    ($t:ty, $def:expr) => {
        impl ResultExt<$t> for Result<$t, Error> {
            fn unwrap_or_throw(self, env: &mut JNIEnv) -> $t {
                match self {
                    Ok(val) => val,
                    Err(err) => {
                        throw_java_exception(env, err);
                        $def
                    },
                }
            }
        }
    };
}

// Same as `impl_result_ext`, but for types carrying a `'local` lifetime.
macro_rules! impl_result_ext_local {
    ($t:ty, $def:expr) => {
        impl<'local> ResultExt<$t> for Result<$t, Error> {
            fn unwrap_or_throw(self, env: &mut JNIEnv) -> $t {
                match self {
                    Ok(val) => val,
                    Err(err) => {
                        throw_java_exception(env, err);
                        $def
                    },
                }
            }
        }
    };
}

impl_result_ext!(i64, 0);
impl_result_ext!(i32, 0);
impl_result_ext!(u8, 0);
impl_result_ext!((), ());
impl_result_ext!(f64, 0.0);
impl_result_ext!(f32, 0.0);
impl_result_ext!(bool, false);
impl_result_ext!(sys::jobject, std::ptr::null_mut());
impl_result_ext!(DataFrame, DataFrame::default());
impl_result_ext!(LazyFrame, LazyFrame::default());
impl_result_ext!(Series, Series::default());
impl_result_ext!(Expr, Expr::default());
impl_result_ext!(DataType, DataType::Null);
impl_result_ext!(PlHashMap<String, String>, PlHashMap::default());
impl_result_ext!(String, String::default());
impl_result_ext!(chrono::NaiveDate, chrono::NaiveDate::default());
impl_result_ext!(chrono::NaiveTime, chrono::NaiveTime::default());
impl_result_ext!(chrono::NaiveDateTime, chrono::NaiveDateTime::default());
impl_result_ext!(Vec<chrono::NaiveDate>, Vec::new());
impl_result_ext!(Vec<chrono::NaiveTime>, Vec::new());
impl_result_ext!(Vec<chrono::NaiveDateTime>, Vec::new());

impl_result_ext_local!(jni::objects::JObject<'local>, jni::objects::JObject::null());
impl_result_ext_local!(
    jni::objects::JString<'local>,
    jni::objects::JString::from(jni::objects::JObject::null())
);
impl_result_ext_local!(
    jni::objects::JClass<'local>,
    jni::objects::JClass::from(jni::objects::JObject::null())
);
impl_result_ext_local!(
    jni::objects::JObjectArray<'local>,
    jni::objects::JObjectArray::from(jni::objects::JObject::null())
);
impl_result_ext_local!(
    jni::objects::JValueGen<jni::objects::JObject<'local>>,
    jni::objects::JValueGen::Void
);

impl<'local, T: jni::objects::TypeArray> ResultExt<jni::objects::JPrimitiveArray<'local, T>>
    for Result<jni::objects::JPrimitiveArray<'local, T>, Error>
{
    fn unwrap_or_throw(self, env: &mut JNIEnv) -> jni::objects::JPrimitiveArray<'local, T> {
        match self {
            Ok(val) => val,
            Err(err) => {
                throw_java_exception(env, err);
                jni::objects::JPrimitiveArray::from(jni::objects::JObject::null())
            },
        }
    }
}
