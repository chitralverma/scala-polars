use anyhow::Context;
use jni::JNIEnv;
use jni::objects::*;
use jni::sys::*;
use jni_fn::jni_fn;
use polars::prelude::*;

use crate::internal_jni::conversion::{AnyValueWrapper, IntoJava};
use crate::internal_jni::utils::{free_ptr, get_n_rows, string_to_j_string};
use crate::utils::error::ResultExt;

#[jni_fn("com.github.chitralverma.polars.internal.jni.row$")]
pub unsafe fn createIterator(_: JNIEnv, _: JClass, df_ptr: *mut DataFrame, nRows: jlong) -> jlong {
    let df = unsafe { &mut *df_ptr };

    let n_rows = get_n_rows(nRows);
    let ri = RowIterator::new(df, n_rows);
    Box::into_raw(Box::new(ri)) as jlong
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.row$")]
pub unsafe fn advanceIterator(
    mut env: JNIEnv,
    _: JClass,
    ri_ptr: *mut RowIterator,
) -> jobjectArray {
    let ri = unsafe { &mut *ri_ptr };
    let adv = ri.advance();

    if let Some(next_avs) = adv {
        let j_array = env
            .new_object_array(next_avs.len() as jsize, "java/lang/Object", JObject::null())
            .context("Failed to initialize array for row values")
            .unwrap_or_throw(&mut env);

        for (i, any_value) in next_avs.into_iter().enumerate() {
            let wrapped = AnyValueWrapper(any_value.clone());
            let java_object = wrapped.into_java(&mut env);
            env.set_object_array_element(&j_array, i as jsize, java_object)
                .context(format!("Failed to set value `{any_value}` in row"))
                .unwrap_or_throw(&mut env);
        }

        j_array.as_raw()
    } else {
        JObjectArray::from(JObject::null()).as_raw()
    }
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.row$")]
pub unsafe fn schemaString(mut env: JNIEnv, _: JClass, ri_ptr: *mut RowIterator) -> jstring {
    let ri = unsafe { &*ri_ptr };

    serde_json::to_string(&ri.schema.to_arrow(CompatLevel::oldest()))
        .map(|schema_string| string_to_j_string(&mut env, schema_string, None::<&str>))
        .context("Failed to serialize schema")
        .unwrap_or_throw(&mut env)
}

#[jni_fn("com.github.chitralverma.polars.internal.jni.row$")]
pub fn free(_: JNIEnv, _: JClass, ptr: jlong) {
    free_ptr::<RowIterator>(ptr);
}

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
