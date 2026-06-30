use anyhow::Error;
use jni::Env;
use jni::errors::ErrorPolicy;

/// Formats an [`anyhow::Error`] with its full `Caused by:` chain so the context attached at each
/// JNI boundary survives to the JVM.
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

/// Throws the closure error as a Java `RuntimeException` carrying the full [`anyhow`] cause chain,
/// returning the type's default sentinel.
///
/// A project-specific counterpart to [`jni::errors::ThrowRuntimeExAndDefault`], needed because that
/// policy is bounded on `std::error::Error` (which [`anyhow::Error`] does not implement) and emits a
/// single flat line rather than the cause chain. A pending exception takes precedence over a new throw.
#[derive(Debug, Default)]
pub struct ThrowRuntimeException;

impl<T: Default> ErrorPolicy<T, Error> for ThrowRuntimeException {
    type Captures<'unowned_env_local: 'native_method, 'native_method> = ();

    fn on_error<'unowned_env_local: 'native_method, 'native_method>(
        env: &mut Env<'unowned_env_local>,
        _cap: &mut Self::Captures<'unowned_env_local, 'native_method>,
        err: Error,
    ) -> jni::errors::Result<T> {
        if env.exception_check() {
            return Ok(T::default());
        }
        // `throw` returns `Err(JavaException)` after raising; discard it and let the exception fly.
        let _ = env.throw(format_nested_error(&err));
        Ok(T::default())
    }

    fn on_panic<'unowned_env_local: 'native_method, 'native_method>(
        env: &mut Env<'unowned_env_local>,
        _cap: &mut Self::Captures<'unowned_env_local, 'native_method>,
        payload: Box<dyn std::any::Any + Send + 'static>,
    ) -> jni::errors::Result<T> {
        if env.exception_check() {
            return Ok(T::default());
        }
        let msg = match payload.downcast::<&'static str>() {
            Ok(s) => (*s).to_string(),
            Err(payload) => match payload.downcast::<String>() {
                Ok(s) => *s,
                Err(_) => "native code panicked".to_string(),
            },
        };
        let _ = env.throw(format!("native code panicked: {msg}"));
        Ok(T::default())
    }
}
