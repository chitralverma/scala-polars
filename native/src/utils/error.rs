use anyhow::Error;
use jni::errors::Result as JniResult;
use jni::JNIEnv;

use crate::internal_jni::utils::find_java_class;

pub fn throw_java_exception(env: &mut JNIEnv, message: &str) -> JniResult<()> {
    // Find the Java exception class
    let exception_class = find_java_class(env, "java/lang/RuntimeException");

    // Throw the exception with the provided message
    env.throw_new(exception_class, message)?;
    Ok(())
}

/// Trait to unwrap `Result` or throw an exception.
pub trait ResultExt<T> {
    fn unwrap_or_throw(self, env: &mut JNIEnv) -> T;
}

impl<T> ResultExt<T> for Result<T, Error> {
    fn unwrap_or_throw(self, env: &mut JNIEnv) -> T {
        match self {
            Ok(val) => val,
            Err(err) => {
                // Map the error to a Java exception
                let _ = throw_java_exception(env, &err.to_string());

                // Exit early by returning a default value for JNI
                env.exception_describe().unwrap_or(());
                std::process::abort();
            },
        }
    }
}
