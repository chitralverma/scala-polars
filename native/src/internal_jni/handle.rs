//! Typed handles for native objects that cross the JNI boundary as `jlong` pointers.
//!
//! Each handle is a `#[repr(transparent)]` newtype over a `*mut T`, layout-compatible with the
//! `jlong` it maps to via `native_method!`'s `type_map = { unsafe XHandle => long }`. Ownership is
//! a hand-maintained contract: `alloc` leaks a box to the JVM, `get` clones the pointee back, and
//! the boxed allocation lives until an explicit `free` (`free_raw`).

use jni::sys::jlong;

/// Cloning lifecycle shared by the pointer handle newtypes; implemented with all-default methods
/// for every `Clone` target via [`impl_clone_handle`].
pub trait Handle<T: Clone>: Copy + Into<jlong> + From<jlong> {
    fn alloc(value: T) -> Self {
        Self::from(Box::into_raw(Box::new(value)) as jlong)
    }

    /// # Safety
    /// The handle must reference a live allocation produced by [`Handle::alloc`].
    fn get(self) -> T {
        let ptr: jlong = self.into();
        unsafe { (*(ptr as *mut T)).clone() }
    }

    /// Drops the boxed allocation behind a raw `jlong` handle; a zero handle is a no-op.
    fn free_raw(raw: jlong) {
        if raw != 0 {
            unsafe {
                let _ = Box::from_raw(raw as *mut T);
            }
        }
    }
}

/// Declares a `#[repr(transparent)]` pointer handle newtype with `jlong` conversions, defaulting to
/// a null pointer (the sentinel returned when an error policy fires).
macro_rules! declare_handle {
    ($name:ident, $target:ty) => {
        #[derive(Clone, Copy)]
        #[repr(transparent)]
        pub struct $name(pub *mut $target);

        impl Default for $name {
            fn default() -> Self {
                $name(std::ptr::null_mut())
            }
        }

        impl From<$name> for jlong {
            fn from(h: $name) -> jlong {
                h.0 as jlong
            }
        }

        impl From<jlong> for $name {
            fn from(v: jlong) -> $name {
                $name(v as *mut $target)
            }
        }
    };
}

/// Adds the cloning [`Handle`] behaviour for a handle whose target is `Clone`.
macro_rules! impl_clone_handle {
    ($name:ident, $target:ty) => {
        impl Handle<$target> for $name {}
    };
}

use polars::prelude::{DataFrame, Expr, LazyFrame, Series};

declare_handle!(ExprHandle, Expr);
declare_handle!(LazyFrameHandle, LazyFrame);
declare_handle!(DataFrameHandle, DataFrame);
declare_handle!(SeriesHandle, Series);
declare_handle!(RowIteratorHandle, crate::internal_jni::row::RowIterator);

impl_clone_handle!(ExprHandle, Expr);
impl_clone_handle!(LazyFrameHandle, LazyFrame);
impl_clone_handle!(DataFrameHandle, DataFrame);
impl_clone_handle!(SeriesHandle, Series);

impl RowIteratorHandle {
    pub fn alloc(value: crate::internal_jni::row::RowIterator) -> Self {
        RowIteratorHandle(Box::into_raw(Box::new(value)))
    }

    /// # Safety
    /// Handle must reference a live allocation from [`RowIteratorHandle::alloc`], not aliased.
    pub unsafe fn as_mut<'a>(self) -> &'a mut crate::internal_jni::row::RowIterator {
        unsafe { &mut *self.0 }
    }

    /// # Safety
    /// Handle must reference a live allocation from [`RowIteratorHandle::alloc`].
    pub unsafe fn as_ref<'a>(self) -> &'a crate::internal_jni::row::RowIterator {
        unsafe { &*self.0 }
    }

    pub fn free_raw(raw: jlong) {
        if raw != 0 {
            unsafe {
                let _ = Box::from_raw(raw as *mut crate::internal_jni::row::RowIterator);
            }
        }
    }
}
