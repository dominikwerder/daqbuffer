use bytes::BufMut;
use std::fmt;

trait WritableValue: fmt::Debug {
    fn put_value(&self, buf: &mut Vec<u8>);
}

impl WritableValue for u32 {
    fn put_value(&self, buf: &mut Vec<u8>) {
        buf.put_u32_le(*self);
    }
}

impl WritableValue for i8 {
    fn put_value(&self, buf: &mut Vec<u8>) {
        buf.put_i8(*self);
    }
}

impl WritableValue for i16 {
    fn put_value(&self, buf: &mut Vec<u8>) {
        buf.put_i16_le(*self);
    }
}

impl WritableValue for i32 {
    fn put_value(&self, buf: &mut Vec<u8>) {
        buf.put_i32_le(*self);
    }
}

impl WritableValue for f32 {
    fn put_value(&self, buf: &mut Vec<u8>) {
        buf.put_f32_le(*self);
    }
}

impl WritableValue for f64 {
    fn put_value(&self, buf: &mut Vec<u8>) {
        buf.put_f64_le(*self);
    }
}
