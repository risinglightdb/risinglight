use crate::types::{ConvertError, DataType, DataTypeExt, DataTypeKind, DataValue};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::ops::{Bound, RangeBounds};

mod data_chunk;
mod iterator;
mod primitive_array;
mod utf8_array;

pub use self::data_chunk::*;
pub use self::iterator::ArrayIter;
pub use self::primitive_array::*;
pub use self::utf8_array::*;

mod internal_ext;
pub use internal_ext::*;

mod shuffle_ext;
pub use shuffle_ext::*;

/// A trait over all array builders.
///
/// `ArrayBuilder` is a trait over all builders. You could build an array with
/// `push` with the help of `ArrayBuilder` trait. The `push` function always
/// accepts reference to an element. e.g. for `PrimitiveArray`,
/// you must do `builder.push(Some(&1))`. For `Utf8Array`, you must do
/// `builder.push(Some("xxx"))`. Note that you don't need to construct a `String`.
///
/// The associated type `Array` is the type of the corresponding array. It is the
/// return type of `finish`.
pub trait ArrayBuilder: Send + Sync + 'static {
    /// Corresponding `Array` of this builder
    type Array: Array<Builder = Self>;

    /// Create a new builder with `capacity`.
    fn new(capacity: usize) -> Self;

    /// Append a value to builder.
    fn push(&mut self, value: Option<&<Self::Array as Array>::Item>);

    /// Append an array to builder.
    fn append(&mut self, other: &Self::Array);

    /// Finish build and return a new array.
    fn finish(self) -> Self::Array;
}

/// A trait over all array.
///
/// `Array` must be built with an `ArrayBuilder`. The array trait provides several
/// unified interface on an array, like `len`, `get` and `iter`.
///
/// The `Builder` associated type is the builder for this array.
/// The `Item` is the item you could retrieve from this array.
///
/// For example, `PrimitiveArray` could return an `Option<&u32>`, and `Utf8Array` will
/// return an `Option<&str>`.
pub trait Array: Sized + Send + Sync + 'static {
    /// Corresponding builder of this array.
    type Builder: ArrayBuilder<Array = Self>;

    /// Type of element in the array.
    type Item: ToOwned + ?Sized;

    /// Retrieve a reference to value.
    fn get(&self, idx: usize) -> Option<&Self::Item>;

    /// Number of items of array.
    fn len(&self) -> usize;

    /// Get iterator of current array.
    fn iter(&self) -> ArrayIter<'_, Self> {
        ArrayIter::new(self)
    }

    /// Check if `Array` is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// An extension trait for [`Array`].
pub trait ArrayExt: Array {
    /// Filter the elements and return a new array.
    fn filter(&self, visibility: impl Iterator<Item = bool>) -> Self;

    /// Return a slice of self for the provided range.
    fn slice(&self, range: impl RangeBounds<usize>) -> Self;
}

impl<A: Array> ArrayExt for A {
    /// Filter the elements and return a new array.
    fn filter(&self, visibility: impl Iterator<Item = bool>) -> Self {
        let mut builder = Self::Builder::new(self.len());
        for (a, visible) in self.iter().zip(visibility) {
            if visible {
                builder.push(a);
            }
        }
        builder.finish()
    }

    /// Return a slice of self for the provided range.
    fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let len = self.len();
        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => len,
        };
        assert!(begin <= end, "range start must not be greater than end");
        assert!(end <= len, "range end out of bounds");

        let mut builder = Self::Builder::new(end - begin);
        for i in begin..end {
            builder.push(self.get(i));
        }
        builder.finish()
    }
}

pub type BoolArray = PrimitiveArray<bool>;
pub type I32Array = PrimitiveArray<i32>;
pub type I64Array = PrimitiveArray<i64>;
pub type F64Array = PrimitiveArray<f64>;

/// Embeds all types of arrays in `array` module.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ArrayImpl {
    Bool(BoolArray),
    // Int16(PrimitiveArray<i16>),
    Int32(I32Array),
    Int64(PrimitiveArray<i64>),
    // Float32(PrimitiveArray<f32>),
    Float64(F64Array),
    Utf8(Utf8Array),
}

pub type BoolArrayBuilder = PrimitiveArrayBuilder<bool>;
pub type I32ArrayBuilder = PrimitiveArrayBuilder<i32>;
pub type I64ArrayBuilder = PrimitiveArrayBuilder<i64>;
pub type F64ArrayBuilder = PrimitiveArrayBuilder<f64>;

/// Embeds all types of array builders in `array` module.
pub enum ArrayBuilderImpl {
    Bool(BoolArrayBuilder),
    // Int16(PrimitiveArrayBuilder<i16>),
    Int32(I32ArrayBuilder),
    Int64(PrimitiveArrayBuilder<i64>),
    // Float32(PrimitiveArrayBuilder<f32>),
    Float64(F64ArrayBuilder),
    Utf8(Utf8ArrayBuilder),
}

/// An error which can be returned when downcasting an [`ArrayImpl`] into a concrete type array.
#[derive(Debug, Clone)]
pub struct TypeMismatch;

macro_rules! impl_into {
    ($x:ty, $y:ident) => {
        impl From<$x> for ArrayImpl {
            fn from(array: $x) -> Self {
                Self::$y(array)
            }
        }

        impl TryFrom<ArrayImpl> for $x {
            type Error = TypeMismatch;

            fn try_from(array: ArrayImpl) -> Result<Self, Self::Error> {
                match array {
                    ArrayImpl::$y(array) => Ok(array),
                    _ => Err(TypeMismatch),
                }
            }
        }

        impl<'a> TryFrom<&'a ArrayImpl> for &'a $x {
            type Error = TypeMismatch;

            fn try_from(array: &'a ArrayImpl) -> Result<Self, Self::Error> {
                match array {
                    ArrayImpl::$y(array) => Ok(array),
                    _ => Err(TypeMismatch),
                }
            }
        }
    };
}

impl_into! { PrimitiveArray<bool>, Bool }
// impl_into! { PrimitiveArray<i16>, Int16 }
impl_into! { PrimitiveArray<i32>, Int32 }
impl_into! { PrimitiveArray<i64>, Int64 }
// impl_into! { PrimitiveArray<f32>, Float32 }
impl_into! { PrimitiveArray<f64>, Float64 }
impl_into! { Utf8Array, Utf8 }

impl ArrayBuilderImpl {
    /// Create a new array builder from data type.
    pub fn new(ty: &DataType) -> Self {
        match ty.kind() {
            DataTypeKind::Boolean => Self::Bool(PrimitiveArrayBuilder::<bool>::new(0)),
            DataTypeKind::Int(_) => Self::Int32(PrimitiveArrayBuilder::<i32>::new(0)),
            DataTypeKind::BigInt(_) => Self::Int64(PrimitiveArrayBuilder::<i64>::new(0)),
            DataTypeKind::Float(_) | DataTypeKind::Double => {
                Self::Float64(PrimitiveArrayBuilder::<f64>::new(0))
            }
            DataTypeKind::Char(_) | DataTypeKind::Varchar(_) | DataTypeKind::String => {
                Self::Utf8(Utf8ArrayBuilder::new(0))
            }
            _ => panic!("unsupported data type"),
        }
    }

    /// Create a new array builder from data value.
    pub fn from_type_of_value(val: &DataValue) -> Self {
        match val {
            DataValue::Bool(_) => Self::Bool(PrimitiveArrayBuilder::<bool>::new(0)),
            DataValue::Int32(_) => Self::Int32(PrimitiveArrayBuilder::<i32>::new(0)),
            DataValue::Int64(_) => Self::Int64(PrimitiveArrayBuilder::<i64>::new(0)),
            DataValue::Float64(_) => Self::Float64(PrimitiveArrayBuilder::<f64>::new(0)),
            DataValue::String(_) => Self::Utf8(Utf8ArrayBuilder::new(0)),
            _ => panic!("unsupported data type"),
        }
    }

    /// Create a new array builder with the same type of given array.
    pub fn from_type_of_array(array: &ArrayImpl) -> Self {
        match array {
            ArrayImpl::Bool(_) => Self::Bool(PrimitiveArrayBuilder::<bool>::new(0)),
            ArrayImpl::Int32(_) => Self::Int32(PrimitiveArrayBuilder::<i32>::new(0)),
            ArrayImpl::Int64(_) => Self::Int64(PrimitiveArrayBuilder::<i64>::new(0)),
            ArrayImpl::Float64(_) => Self::Float64(PrimitiveArrayBuilder::<f64>::new(0)),
            ArrayImpl::Utf8(_) => Self::Utf8(Utf8ArrayBuilder::new(0)),
        }
    }

    /// Appends an element to the back of array.
    pub fn push(&mut self, v: &DataValue) {
        match (self, v) {
            (Self::Bool(a), DataValue::Bool(v)) => a.push(Some(v)),
            (Self::Int64(a), DataValue::Int64(v)) => a.push(Some(v)),
            (Self::Int32(a), DataValue::Int32(v)) => a.push(Some(v)),
            (Self::Float64(a), DataValue::Float64(v)) => a.push(Some(v)),
            (Self::Utf8(a), DataValue::String(v)) => a.push(Some(v)),
            (Self::Bool(a), DataValue::Null) => a.push(None),
            (Self::Int32(a), DataValue::Null) => a.push(None),
            (Self::Int64(a), DataValue::Null) => a.push(None),
            (Self::Float64(a), DataValue::Null) => a.push(None),
            (Self::Utf8(a), DataValue::Null) => a.push(None),
            _ => panic!("failed to push value: type mismatch"),
        }
    }

    /// Appends an element in string.
    pub fn push_str(&mut self, s: &str) -> Result<(), ConvertError> {
        let null = s.is_empty();
        match self {
            Self::Bool(a) if null => a.push(None),
            Self::Int32(a) if null => a.push(None),
            Self::Int64(a) if null => a.push(None),
            Self::Float64(a) if null => a.push(None),
            Self::Utf8(a) if null => a.push(None),
            Self::Bool(a) => a.push(Some(
                &s.parse::<bool>()
                    .map_err(|e| ConvertError::ParseBool(s.to_string(), e))?,
            )),
            Self::Int32(a) => a.push(Some(
                &s.parse::<i32>()
                    .map_err(|e| ConvertError::ParseInt(s.to_string(), e))?,
            )),
            Self::Int64(a) => a.push(Some(
                &s.parse::<i64>()
                    .map_err(|e| ConvertError::ParseInt(s.to_string(), e))?,
            )),
            Self::Float64(a) => a.push(Some(
                &s.parse::<f64>()
                    .map_err(|e| ConvertError::ParseFloat(s.to_string(), e))?,
            )),
            Self::Utf8(a) => a.push(Some(s)),
        }
        Ok(())
    }

    /// Finish build and return a new array.
    pub fn finish(self) -> ArrayImpl {
        match self {
            Self::Bool(a) => ArrayImpl::Bool(a.finish()),
            Self::Int32(a) => ArrayImpl::Int32(a.finish()),
            Self::Int64(a) => ArrayImpl::Int64(a.finish()),
            Self::Float64(a) => ArrayImpl::Float64(a.finish()),
            Self::Utf8(a) => ArrayImpl::Utf8(a.finish()),
        }
    }

    /// Appends a DataArrayImpl
    pub fn append(&mut self, array_impl: &ArrayImpl) {
        match (self, array_impl) {
            (Self::Bool(builder), ArrayImpl::Bool(arr)) => builder.append(arr),
            (Self::Int32(builder), ArrayImpl::Int32(arr)) => builder.append(arr),
            (Self::Int64(builder), ArrayImpl::Int64(arr)) => builder.append(arr),
            (Self::Float64(builder), ArrayImpl::Float64(arr)) => builder.append(arr),
            (Self::Utf8(builder), ArrayImpl::Utf8(arr)) => builder.append(arr),
            _ => panic!("failed to push value: type mismatch"),
        }
    }
}

impl ArrayImpl {
    /// Get the value and convert it to string.
    pub fn get_to_string(&self, idx: usize) -> String {
        match self {
            Self::Bool(a) => a.get(idx).map(|v| v.to_string()),
            Self::Int32(a) => a.get(idx).map(|v| v.to_string()),
            Self::Int64(a) => a.get(idx).map(|v| v.to_string()),
            Self::Float64(a) => a.get(idx).map(|v| v.to_string()),
            Self::Utf8(a) => a.get(idx).map(|v| v.to_string()),
        }
        .unwrap_or_else(|| "NULL".into())
    }

    /// Get the value at the given index.
    pub fn get(&self, idx: usize) -> DataValue {
        match self {
            Self::Bool(a) => match a.get(idx) {
                Some(val) => DataValue::Bool(*val),
                None => DataValue::Null,
            },
            Self::Int32(a) => match a.get(idx) {
                Some(val) => DataValue::Int32(*val),
                None => DataValue::Null,
            },
            Self::Int64(a) => match a.get(idx) {
                Some(val) => DataValue::Int64(*val),
                None => DataValue::Null,
            },
            Self::Float64(a) => match a.get(idx) {
                Some(val) => DataValue::Float64(*val),
                None => DataValue::Null,
            },
            Self::Utf8(a) => match a.get(idx) {
                Some(val) => DataValue::String(val.to_string()),
                None => DataValue::Null,
            },
        }
    }

    /// Number of items of array.
    pub fn len(&self) -> usize {
        match self {
            Self::Bool(a) => a.len(),
            Self::Int32(a) => a.len(),
            Self::Int64(a) => a.len(),
            Self::Float64(a) => a.len(),
            Self::Utf8(a) => a.len(),
        }
    }

    /// Check if array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Filter the elements and return a new array.
    pub fn filter(&self, visibility: impl Iterator<Item = bool>) -> Self {
        match self {
            Self::Bool(a) => Self::Bool(a.filter(visibility)),
            Self::Int32(a) => Self::Int32(a.filter(visibility)),
            Self::Int64(a) => Self::Int64(a.filter(visibility)),
            Self::Float64(a) => Self::Float64(a.filter(visibility)),
            Self::Utf8(a) => Self::Utf8(a.filter(visibility)),
        }
    }

    /// Return a slice of self for the provided range.
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        match self {
            Self::Bool(a) => Self::Bool(a.slice(range)),
            Self::Int32(a) => Self::Int32(a.slice(range)),
            Self::Int64(a) => Self::Int64(a.slice(range)),
            Self::Float64(a) => Self::Float64(a.slice(range)),
            Self::Utf8(a) => Self::Utf8(a.slice(range)),
        }
    }

    /// Get the type of value.
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Bool(_) => Some(DataTypeKind::Boolean.not_null()),
            Self::Int32(_) => Some(DataTypeKind::Int(None).not_null()),
            Self::Int64(_) => Some(DataTypeKind::BigInt(None).not_null()),
            Self::Float64(_) => Some(DataTypeKind::Double.not_null()),
            Self::Utf8(_) => Some(DataTypeKind::String.not_null()),
        }
    }
}

/// Create a single element array from data value.
impl From<&DataValue> for ArrayImpl {
    fn from(val: &DataValue) -> Self {
        match val {
            &DataValue::Bool(v) => Self::Bool([v].into_iter().collect()),
            &DataValue::Int32(v) => Self::Int32([v].into_iter().collect()),
            &DataValue::Int64(v) => Self::Int64([v].into_iter().collect()),
            &DataValue::Float64(v) => Self::Float64([v].into_iter().collect()),
            DataValue::String(v) => Self::Utf8([Some(v)].into_iter().collect()),
            DataValue::Null => panic!("can not build array from NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter() {
        let array: PrimitiveArray<i32> = (0..=60).map(Some).collect();
        let array: PrimitiveArray<i32> = array
            .iter()
            .filter(|x| *x.unwrap_or(&0) >= 60)
            .map(|x| x.cloned())
            .collect();
        assert_eq!(
            array.iter().map(|x| x.cloned()).collect::<Vec<_>>(),
            vec![Some(60)]
        );
    }

    use crate::types::NativeType;
    use num_traits::cast::AsPrimitive;
    use num_traits::ops::checked::CheckedAdd;

    fn vec_add<T1, T2, T3>(a: &PrimitiveArray<T1>, b: &PrimitiveArray<T2>) -> PrimitiveArray<T3>
    where
        T1: NativeType + AsPrimitive<T3>,
        T2: NativeType + AsPrimitive<T3>,
        T3: NativeType + CheckedAdd,
    {
        assert_eq!(a.len(), b.len());
        a.iter()
            .zip(b.iter())
            .map(|(a, b)| match (a, b) {
                (Some(a), Some(b)) => Some(a.as_() + b.as_()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn test_vectorized_add() {
        let array1 = (0i32..=60).map(Some).collect();
        let array2 = (0i16..=60).map(Some).collect();

        let final_array = vec_add(&array1, &array2) as PrimitiveArray<i64>;
        assert_eq!(
            final_array.iter().map(|x| x.cloned()).collect::<Vec<_>>(),
            (0i64..=60).map(|i| Some(i * 2)).collect::<Vec<_>>()
        );
    }
}
