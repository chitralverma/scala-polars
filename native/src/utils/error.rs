use anyhow::Error;
use jni::errors::Result as JniResult;
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

pub fn throw_java_exception(env: &mut JNIEnv, err: Error) -> JniResult<()> {
    // Resolved directly (not via find_java_class) to avoid recursion.
    let exception_class = env.find_class("java/lang/RuntimeException")?;
    env.throw_new(exception_class, format_nested_error(&err))?;
    Ok(())
}

/// Trait to unwrap `Result` or throw an exception.
pub trait ResultExt<T> {
    fn unwrap_or_throw(self, env: &mut JNIEnv) -> T;
}

// Throws the error as a Java exception, then returns a default sentinel for the JNI boundary.
macro_rules! impl_result_ext {
    ($t:ty, $def:expr) => {
        impl ResultExt<$t> for Result<$t, Error> {
            fn unwrap_or_throw(self, env: &mut JNIEnv) -> $t {
                match self {
                    Ok(val) => val,
                    Err(err) => {
                        if !env.exception_check().unwrap_or(false) {
                            let _ = throw_java_exception(env, err);
                        }
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

impl<'local> ResultExt<jni::objects::JObject<'local>>
    for Result<jni::objects::JObject<'local>, Error>
{
    fn unwrap_or_throw(self, env: &mut JNIEnv) -> jni::objects::JObject<'local> {
        match self {
            Ok(val) => val,
            Err(err) => {
                if !env.exception_check().unwrap_or(false) {
                    let _ = throw_java_exception(env, err);
                }
                jni::objects::JObject::null()
            },
        }
    }
}

impl<'local> ResultExt<jni::objects::JString<'local>>
    for Result<jni::objects::JString<'local>, Error>
{
    fn unwrap_or_throw(self, env: &mut JNIEnv) -> jni::objects::JString<'local> {
        match self {
            Ok(val) => val,
            Err(err) => {
                if !env.exception_check().unwrap_or(false) {
                    let _ = throw_java_exception(env, err);
                }
                jni::objects::JString::from(jni::objects::JObject::null())
            },
        }
    }
}

impl<'local> ResultExt<jni::objects::JClass<'local>>
    for Result<jni::objects::JClass<'local>, Error>
{
    fn unwrap_or_throw(self, env: &mut JNIEnv) -> jni::objects::JClass<'local> {
        match self {
            Ok(val) => val,
            Err(err) => {
                if !env.exception_check().unwrap_or(false) {
                    let _ = throw_java_exception(env, err);
                }
                jni::objects::JClass::from(jni::objects::JObject::null())
            },
        }
    }
}

impl<'local> ResultExt<jni::objects::JObjectArray<'local>>
    for Result<jni::objects::JObjectArray<'local>, Error>
{
    fn unwrap_or_throw(self, env: &mut JNIEnv) -> jni::objects::JObjectArray<'local> {
        match self {
            Ok(val) => val,
            Err(err) => {
                if !env.exception_check().unwrap_or(false) {
                    let _ = throw_java_exception(env, err);
                }
                jni::objects::JObjectArray::from(jni::objects::JObject::null())
            },
        }
    }
}

impl<'local, T: jni::objects::TypeArray> ResultExt<jni::objects::JPrimitiveArray<'local, T>>
    for Result<jni::objects::JPrimitiveArray<'local, T>, Error>
{
    fn unwrap_or_throw(self, env: &mut JNIEnv) -> jni::objects::JPrimitiveArray<'local, T> {
        match self {
            Ok(val) => val,
            Err(err) => {
                if !env.exception_check().unwrap_or(false) {
                    let _ = throw_java_exception(env, err);
                }
                jni::objects::JPrimitiveArray::from(jni::objects::JObject::null())
            },
        }
    }
}

impl<'local> ResultExt<jni::objects::JValueGen<jni::objects::JObject<'local>>>
    for Result<jni::objects::JValueGen<jni::objects::JObject<'local>>, Error>
{
    fn unwrap_or_throw(
        self,
        env: &mut JNIEnv,
    ) -> jni::objects::JValueGen<jni::objects::JObject<'local>> {
        match self {
            Ok(val) => val,
            Err(err) => {
                if !env.exception_check().unwrap_or(false) {
                    let _ = throw_java_exception(env, err);
                }
                jni::objects::JValueGen::Void
            },
        }
    }
}
