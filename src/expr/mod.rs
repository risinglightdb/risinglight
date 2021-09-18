//! Expression framework of RisingLight

use std::convert::{TryFrom, TryInto};
use std::marker::PhantomData;

use crate::array::{Array, ArrayBuilder, ArrayImpl, ScalarRef};

/// `BinaryExpression` is a trait over all binary functions.
pub trait BinaryExpression {
    /// evaluate a chunk
    fn eval_chunk(&self, input1: &ArrayImpl, input2: &ArrayImpl) -> ArrayImpl;
}

/// `BinaryVectorizedExpression` automatically transforms a scalar function
/// of concrete type to a vectorized function of erased type.
///
/// # Generic Types
/// * `I1`: input left type
/// * `I2`: input right type
/// * `O`: output type
/// * `F`: scalar function type
pub struct BinaryVectorizedExpression<I1, I2, O, F>
where
    I1: Array,
    I2: Array,
    O: Array,
    F: Fn(Option<&I1::Item>, Option<&I2::Item>) -> Option<<O::Item as ScalarRef>::OwnedType>,
{
    func: F,
    _phantom: PhantomData<(I1, I2, O)>,
}

impl<I1, I2, O, F> BinaryVectorizedExpression<I1, I2, O, F>
where
    I1: Array,
    I2: Array,
    O: Array,
    F: Fn(Option<&I1::Item>, Option<&I2::Item>) -> Option<<O::Item as ScalarRef>::OwnedType>,
{
    pub fn new(func: F) -> Self {
        Self {
            func,
            _phantom: PhantomData,
        }
    }
}

impl<I1, I2, O, F> BinaryExpression for BinaryVectorizedExpression<I1, I2, O, F>
where
    I1: Array,
    for<'a> &'a I1: TryFrom<&'a ArrayImpl>,
    for<'a> <&'a I1 as std::convert::TryFrom<&'a ArrayImpl>>::Error: std::fmt::Debug,
    I2: Array,
    for<'b> &'b I2: TryFrom<&'b ArrayImpl>,
    for<'b> <&'b I2 as std::convert::TryFrom<&'b ArrayImpl>>::Error: std::fmt::Debug,
    O: Array + Into<ArrayImpl>,
    F: Fn(Option<&I1::Item>, Option<&I2::Item>) -> Option<<O::Item as ScalarRef>::OwnedType>,
{
    fn eval_chunk(&self, input1: &ArrayImpl, input2: &ArrayImpl) -> ArrayImpl {
        let input1: &I1 = input1
            .try_into()
            .expect("failed to convert array to concrete type");
        let input2: &I2 = input2
            .try_into()
            .expect("failed to convert array to concrete type");
        assert_eq!(input1.len(), input2.len());
        let mut builder = O::Builder::new(input1.len());
        for (p1, p2) in input1.iter().zip(input2.iter()) {
            builder.push(
                (self.func)(p1, p2)
                    .as_ref()
                    .map(|x| ScalarRef::from_scalar_owned(x)),
            );
        }
        builder.finish().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::I32Array;
    use std::iter::FromIterator;

    #[test]
    fn test_vec_add() {
        let expr = BinaryVectorizedExpression::<I32Array, I32Array, I32Array, _>::new(|x, y| {
            x.and_then(|x| y.map(|y| x + y))
        });
        let result: I32Array = expr
            .eval_chunk(
                &ArrayImpl::Int32(I32Array::from_iter([Some(1), Some(2), Some(3)])),
                &ArrayImpl::Int32(I32Array::from_iter([Some(1), Some(2), Some(3)])),
            )
            .try_into()
            .unwrap();
        assert_eq!(
            result.iter().map(|x| x.cloned()).collect::<Vec<_>>(),
            [Some(2), Some(4), Some(6)]
        );
    }
}
