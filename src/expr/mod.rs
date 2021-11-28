//! Expression framework of RisingLight

use std::borrow::Borrow;
use std::convert::{TryFrom, TryInto};
use std::marker::PhantomData;

use crate::array::{Array, ArrayBuilder, ArrayImpl, TypeMismatch};

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
    F: Fn(Option<&I1::Item>, Option<&I2::Item>) -> Option<<O::Item as ToOwned>::Owned>,
{
    func: F,
    _phantom: PhantomData<(I1, I2, O)>,
}

impl<I1, I2, O, F> BinaryVectorizedExpression<I1, I2, O, F>
where
    I1: Array,
    I2: Array,
    O: Array,
    F: Fn(Option<&I1::Item>, Option<&I2::Item>) -> Option<<O::Item as ToOwned>::Owned>,
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
    for<'a> &'a I1: TryFrom<&'a ArrayImpl, Error = TypeMismatch>,
    I2: Array,
    for<'b> &'b I2: TryFrom<&'b ArrayImpl, Error = TypeMismatch>,
    O: Array + Into<ArrayImpl>,
    F: Fn(Option<&I1::Item>, Option<&I2::Item>) -> Option<<O::Item as ToOwned>::Owned>,
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
            builder.push((self.func)(p1, p2).as_ref().map(|x| x.borrow()));
        }
        builder.finish().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{I32Array, Utf8Array};
    use std::iter::FromIterator;

    #[test]
    fn test_vec_add() {
        let expr = BinaryVectorizedExpression::<I32Array, I32Array, I32Array, _>::new(|x, y| {
            x.and_then(|x| y.map(|y| x + y))
        });
        let result: I32Array = expr
            .eval_chunk(
                &I32Array::from_iter([Some(1), Some(2), Some(3)]).into(),
                &I32Array::from_iter([Some(1), Some(2), Some(3)]).into(),
            )
            .try_into()
            .unwrap();
        assert_eq!(
            result.iter().map(|x| x.cloned()).collect::<Vec<_>>(),
            [Some(2), Some(4), Some(6)]
        );
    }

    #[test]
    fn test_vec_concat() {
        let expr = BinaryVectorizedExpression::<Utf8Array, I32Array, Utf8Array, _>::new(|x, y| {
            x.and_then(|x| y.map(|y| format!("{}{}", x, y)))
        });
        let result: Utf8Array = expr
            .eval_chunk(
                &Utf8Array::from_iter([Some("1"), Some("2"), Some("3")]).into(),
                &I32Array::from_iter([Some(1), Some(2), Some(3)]).into(),
            )
            .try_into()
            .unwrap();
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            [Some("11"), Some("22"), Some("33")]
        );
    }
}
