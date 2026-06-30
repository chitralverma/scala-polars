use anyhow::Error;
use jni::Env;
use jni::errors::ErrorPolicy;

/// Formats an [`anyhow::Error`] together with its full `Caused by:` chain so the
/// nested context attached at each JNI boundary survives the trip to the JVM.
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

/// [`ErrorPolicy`] that throws the closure error as a Java `RuntimeException`,
/// carrying the full [`anyhow`] cause chain, and returns the type's default
/// sentinel for the JNI boundary.
///
/// This is the project-specific counterpart to [`jni::errors::ThrowRuntimeExAndDefault`].
/// We provide our own so the policy is generic over [`anyhow::Error`] (which does
/// not implement [`std::error::Error`]) and so the thrown message preserves the
/// nested context rather than a single flat line.
///
/// If an exception is already pending when an error or panic occurs, that
/// exception takes precedence: re-throwing over a pending exception fails and
/// would hide the original.
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
        // `Env::throw` returns `Err(Error::JavaException)` after creating the pending
        // exception; we intentionally let that exception propagate to Java and do not
        // surface the sentinel error here.
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
