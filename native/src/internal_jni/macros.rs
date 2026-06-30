//! Shared declarative macros for the JNI layer. Per-module `*_method!` wrappers (defined in each
//! module) inject that module's `java_type`/`error_policy`/`type_map`; the macros here fold both
//! the `NativeMethod` constant and impl function for entry points whose body is identical across
//! modules (currently `free`).

/// Declares the `free(ptr: Long)` native method (constant + impl) for `$handle`. Expands in the
/// caller's scope, so `ThrowRuntimeException` and the handle's `Handle` trait must be imported there.
macro_rules! decl_free {
    ($const_name:ident, $java_type:literal, $handle:ty) => {
        const $const_name: ::jni::NativeMethod = ::jni::native_method! {
            java_type = $java_type,
            error_policy = ThrowRuntimeException,
            extern fn free(ptr: jlong),
        };

        fn free<'local>(
            _env: &mut ::jni::Env<'local>,
            _this: ::jni::objects::JObject<'local>,
            ptr: ::jni::sys::jlong,
        ) -> ::anyhow::Result<()> {
            <$handle>::free_raw(ptr);
            Ok(())
        }
    };
}

pub(crate) use decl_free;
