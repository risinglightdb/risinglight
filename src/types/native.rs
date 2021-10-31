use std::fmt::Debug;

pub trait NativeType:
    PartialOrd + PartialEq + Debug + Copy + Send + Sync + Sized + Default + 'static
{
    const ZERO: Self;
}

macro_rules! impl_native {
    ($($t:ty),*) => {
        $(impl NativeType for $t { const ZERO: Self = 0 as Self; })*
    }
}
impl_native!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64);

impl NativeType for bool {
    const ZERO: Self = false;
}
