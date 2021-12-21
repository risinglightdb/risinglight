use std::iter::Iterator;
use std::marker::PhantomData;

use super::Array;

/// An iterator over the elements of an [`Array`].
#[derive(Clone)]
pub struct ArrayIter<'a, A: Array> {
    array: &'a A,
    index: usize,
    _phantom: PhantomData<&'a usize>,
}

impl<'a, A: Array> ArrayIter<'a, A> {
    /// Create an iterator over array.
    pub fn new(array: &'a A) -> Self {
        Self {
            array,
            index: 0,
            _phantom: PhantomData,
        }
    }
}

impl<'a, A: Array> Iterator for ArrayIter<'a, A> {
    type Item = Option<&'a A::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.array.len() {
            None
        } else {
            let item = self.array.get(self.index);
            self.index += 1;
            Some(item)
        }
    }
}
