use jni::objects::GlobalRef;
use polars::prelude::*;

#[derive(Clone)]
pub struct JSeries {
    pub series: Series,
    _callback: GlobalRef,
}

impl JSeries {
    pub fn new(series: Series, callback: GlobalRef) -> JSeries {
        JSeries {
            series,
            _callback: callback,
        }
    }

    pub fn show(&self) {
        println!("{:?}", self.series)
    }
}
