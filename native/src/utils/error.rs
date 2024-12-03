use anyhow::Error;
use jni::errors::Result as JniResult;
use jni::JNIEnv;

use crate::internal_jni::utils::find_java_class;

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
    // Find the Java exception class
    let exception_class = find_java_class(env, "java/lang/RuntimeException");

    // Throw the exception with the provided message
    env.throw_new(exception_class, format_nested_error(&err))?;
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
                let _ = throw_java_exception(env, err);

                // Exit early by returning a default value for JNI
                env.exception_describe().unwrap_or(());
                std::process::abort();
            },
        }
    }
}
