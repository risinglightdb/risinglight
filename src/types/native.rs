use std::fmt::Debug;
use std::io::Write;

pub trait NativeType:
    PartialOrd + PartialEq + Debug + Copy + Send + Sync + Sized + Default + 'static
{
}

impl NativeType for i16 {}

impl NativeType for i32 {}

impl NativeType for i64 {}

impl NativeType for f32 {}

impl NativeType for f64 {}

impl NativeType for u8 {}

impl NativeType for u16 {}

impl NativeType for u32 {}

impl NativeType for u64 {}

impl NativeType for bool {}
