use jni::objects::GlobalRef;
use polars::prelude::*;

#[derive(Clone)]
pub struct JDataFrame {
    pub df: DataFrame,
    _callback: GlobalRef,
}

impl JDataFrame {
    pub fn new(df: DataFrame, callback: GlobalRef) -> JDataFrame {
        JDataFrame {
            df,
            _callback: callback,
        }
    }

    pub fn show(&self) {
        println!("{:?}", self.df)
    }
}
