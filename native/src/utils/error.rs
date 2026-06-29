use anyhow::Error;
use jni::errors::Result as JniResult;
use jni::{JNIEnv, sys};
use polars::prelude::{DataFrame, Expr, LazyFrame, Series};

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
    // Find the Java exception class directly to avoid recursion
    let exception_class = env.find_class("java/lang/RuntimeException")?;

    // Throw the exception with the provided message
    env.throw_new(exception_class, format_nested_error(&err))?;
    Ok(())
}

/// Trait to unwrap `Result` or throw an exception.
pub trait ResultExt<T> {
    fn unwrap_or_throw(self, env: &mut JNIEnv) -> T;
}

// 1. Default/fallback implementation: throw exception and abort.
impl<T> ResultExt<T> for Result<T, Error> {
    default fn unwrap_or_throw(self, env: &mut JNIEnv) -> T {
        match self {
            Ok(val) => val,
            Err(err) => {
                if !env.exception_check().unwrap_or(false) {
                    let _ = throw_java_exception(env, err);
                }
                std::process::abort();
            },
        }
    }
}

// 2. Specialized implementations for primitive/Default types:
macro_rules! impl_result_ext_safe {
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

impl_result_ext_safe!(i64, 0);
impl_result_ext_safe!(i32, 0);
impl_result_ext_safe!(u8, 0);
impl_result_ext_safe!((), ());
impl_result_ext_safe!(f64, 0.0);
impl_result_ext_safe!(f32, 0.0);
impl_result_ext_safe!(bool, false);
impl_result_ext_safe!(sys::jobject, std::ptr::null_mut());
impl_result_ext_safe!(DataFrame, DataFrame::default());
impl_result_ext_safe!(LazyFrame, LazyFrame::default());
impl_result_ext_safe!(Series, Series::default());
impl_result_ext_safe!(Expr, Expr::default());

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

pub fn catch_unwind_or_throw<T, F>(env: &mut JNIEnv, f: F) -> T
where
    F: FnOnce() -> T + std::panic::UnwindSafe,
    Result<T, Error>: ResultExt<T>,
{
    let result = std::panic::catch_unwind(f);
    match result {
        Ok(val) => val,
        Err(err) => {
            let msg = if let Some(s) = err.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = err.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown Rust panic".to_string()
            };
            let anyhow_err = anyhow::anyhow!("Rust Panic: {}", msg);
            Err(anyhow_err).unwrap_or_throw(env)
        },
    }
}
