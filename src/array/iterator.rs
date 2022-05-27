// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::iter::{Iterator, TrustedLen};
use std::marker::PhantomData;

use super::Array;

/// An iterator over the elements of an [`Array`].
#[derive(Clone)]
pub struct ArrayIter<'a, A: Array> {
    data: &'a A,
    pos: usize,
    _phantom: PhantomData<&'a usize>,
}

impl<'a, A: Array> ArrayIter<'a, A> {
    pub fn new(data: &'a A) -> Self {
        Self {
            data,
            pos: 0,
            _phantom: PhantomData,
        }
    }
}

impl<'a, A: Array> Iterator for ArrayIter<'a, A> {
    type Item = Option<&'a A::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            None
        } else {
            let item = self.data.get(self.pos);
            self.pos += 1;
            Some(item)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.data.len() - self.pos;
        (exact, Some(exact))
    }
}

pub struct NonNullArrayIter<'a, A: Array> {
    data: &'a A,
    pos: usize,
    _phantom: PhantomData<&'a usize>,
}

impl<'a, A: Array> NonNullArrayIter<'a, A> {
    pub fn new(data: &'a A) -> Self {
        Self {
            data,
            pos: 0,
            _phantom: PhantomData,
        }
    }
}

impl<'a, A: Array> Iterator for NonNullArrayIter<'a, A> {
    type Item = &'a A::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            None
        } else {
            let value = self.data.get(self.pos);
            self.pos += 1;
            value
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.data.len() - self.pos;
        (exact, Some(exact))
    }
}

unsafe impl<A: Array> TrustedLen for ArrayIter<'_, A> {}

unsafe impl<A: Array> TrustedLen for NonNullArrayIter<'_, A> {}
