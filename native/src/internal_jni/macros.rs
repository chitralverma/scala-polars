//! Shared declarative macros for folding the `native_method!` const + impl-fn pairs.
//!
//! The per-module `*_method!` wrappers (defined in each JNI module) inject that module's
//! `java_type`, `error_policy`, and handle `type_map` so each entry point collapses to a single
//! `native_method!` shorthand. The macros here cover entry points whose bodies are identical
//! across modules (currently `free`), folding both the constant and the implementation function.

/// Declares the `free(ptr: Long): Unit` native method for a handle type: emits both the
/// `NativeMethod` constant and the implementation that reclaims the boxed allocation.
///
/// `$java_type` is the binary name of the owning Scala companion object; `$handle` is the handle
/// whose `free_raw` reclaims the pointer. Expands in the caller's scope, so `ThrowRuntimeException`
/// and the handle's `Handle` trait must be imported there.
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
