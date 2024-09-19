use core::fmt;
use std::collections::VecDeque;

pub trait PreviewRange {
    fn preview(&self) -> &dyn fmt::Debug;
}

impl<T> PreviewRange for VecDeque<T> {
    fn preview(&self) -> &dyn fmt::Debug {
        todo!()
    }
}

pub struct VecPreview<'a> {
    c: &'a dyn PreviewRange,
}

impl<'a> VecPreview<'a> {
    pub fn new(c: &'a dyn PreviewRange) -> Self {
        Self { c }
    }
}

impl<'a> fmt::Debug for VecPreview<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self.c.preview())
    }
}
