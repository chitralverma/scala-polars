use anyhow::Context;
use jni::objects::{JObject, JObjectArray, JString};
use jni::sys::jlong;
use jni::{Env, NativeMethod, native_method};
use polars::prelude::*;

use crate::internal_jni::conversion::{AnyValueWrapper, IntoJava};
use crate::internal_jni::handle::{DataFrameHandle, Handle, RowIteratorHandle};
use crate::internal_jni::utils::get_n_rows;
use crate::utils::error::ThrowRuntimeException;

/// Wraps [`native_method!`] with the `row$` config common to every entry point in this module.
macro_rules! row_method {
    ($($tt:tt)*) => {
        native_method! {
            java_type = "com.github.chitralverma.polars.internal.jni.row$",
            error_policy = ThrowRuntimeException,
            type_map = { unsafe DataFrameHandle => long, unsafe RowIteratorHandle => long },
            $($tt)*
        }
    };
}

const CREATE_ITERATOR_METHOD: NativeMethod = row_method! {
    extern fn create_iterator(df: DataFrameHandle, n_rows: jlong) -> RowIteratorHandle,
    name = "createIterator",
};

fn create_iterator<'local>(
    _env: &mut Env<'local>,
    _this: JObject<'local>,
    df: DataFrameHandle,
    n_rows: jlong,
) -> anyhow::Result<RowIteratorHandle> {
    let mut df = df.get();
    let ri = RowIterator::new(&mut df, get_n_rows(n_rows));
    Ok(RowIteratorHandle::alloc(ri))
}

const ADVANCE_ITERATOR_METHOD: NativeMethod = row_method! {
    extern fn advance_iterator(ri: RowIteratorHandle) -> [java.lang.Object],
    name = "advanceIterator",
};

fn advance_iterator<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ri: RowIteratorHandle,
) -> anyhow::Result<JObjectArray<'local, JObject<'local>>> {
    let ri = unsafe { ri.as_mut() };

    if let Some(next_avs) = ri.advance() {
        let j_array = JObjectArray::<JObject>::new(env, next_avs.len(), JObject::null())
            .context("Failed to initialize array for row values")?;

        for (i, any_value) in next_avs.into_iter().enumerate() {
            let wrapped = AnyValueWrapper(any_value.clone());
            let java_object = wrapped.try_into_java(env)?;
            j_array
                .set_element(env, i, java_object)
                .context(format!("Failed to set value `{any_value}` in row"))?;
        }

        Ok(j_array)
    } else {
        // Returning a null array signals end-of-iteration: the Scala caller wraps the result in
        // `Option(value)`, which is `None` only for a JVM-null reference (an empty array would be
        // `Some` and never terminate the iterator).
        Ok(unsafe { JObjectArray::<JObject>::from_raw(env, std::ptr::null_mut()) })
    }
}

const SCHEMA_STRING_METHOD: NativeMethod = row_method! {
    extern fn schema_string(ri: RowIteratorHandle) -> JString,
    name = "schemaString",
};

fn schema_string<'local>(
    env: &mut Env<'local>,
    _this: JObject<'local>,
    ri: RowIteratorHandle,
) -> anyhow::Result<JString<'local>> {
    let ri = unsafe { ri.as_ref() };

    let schema_string = serde_json::to_string(&ri.schema.to_arrow(CompatLevel::oldest()))
        .context("Failed to serialize schema")?;

    JString::from_str(env, schema_string).context("Failed to build schema string")
}

decl_free!(
    FREE_METHOD,
    "com.github.chitralverma.polars.internal.jni.row$",
    RowIteratorHandle
);

pub struct RowIterator {
    columns: Vec<Column>,
    start: usize,
    pub end: usize,
    pub schema: SchemaRef,
}

impl RowIterator {
    pub fn new(data_frame: &mut DataFrame, end: Option<usize>) -> Self {
        data_frame.align_chunks_par();
        let columns = data_frame.columns().to_vec();
        let schema = data_frame.schema();

        Self {
            columns,
            start: 0,
            schema: schema.clone(),
            end: std::cmp::min(end.unwrap_or(data_frame.height()), data_frame.height()),
        }
    }

    pub fn advance(&mut self) -> Option<Vec<AnyValue<'_>>> {
        if self.start < self.end {
            let mut row = Vec::with_capacity(self.columns.len());
            for col in &self.columns {
                row.push(
                    col.get(self.start)
                        .expect("RowIterator invariant violated: column index out of bounds"),
                );
            }

            self.start += 1;

            Some(row)
        } else {
            None
        }
    }
}

/// All native methods exported by this module.
pub const METHODS: &[NativeMethod] = &[
    CREATE_ITERATOR_METHOD,
    ADVANCE_ITERATOR_METHOD,
    SCHEMA_STRING_METHOD,
    FREE_METHOD,
];
