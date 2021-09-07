// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines a bitmap, which is used to track which values in an Arrow array are null.
//! This is called a "validity bitmap" in the Arrow documentation.
//! This file is adapted from [arrow-rs](https://github.com/apache/arrow-rs)
use super::*;
use std::mem;
use std::ops::BitAnd;
use std::ops::BitOr;

use crate::array::bit_util;

#[derive(Debug)]
pub struct Bitmap {
    pub bits: Buffer,

    // The useful bits in the bitmap. The total number of bits will usually
    // be larger than the useful bits due to byte-padding.
    num_bits: usize,
}

impl Bitmap {
    pub fn new(num_bits: usize) -> Result<Self, ArrayError> {
        let len = Bitmap::num_of_bytes(num_bits);
        Ok(Bitmap {
            bits: Buffer::try_from(&vec![0xFF; len])?,
            num_bits,
        })
    }
    /*
    pub fn from_bool_array(bools: &BoolArray) -> Result<Self, ArrayError> {
        let mut buffer = Buffer::new(Bitmap::num_of_bytes(bools.len()))?;
        let data = buffer.as_slice_mut();
        for (idx, value) in (bools.as_iter()?).enumerate() {
            if let Some(true) = value {
                bit_util::set_bit(data, idx);
            } else {
                bit_util::unset_bit(data, idx);
            }
        }
        Ok(Self {
            bits: buffer,
            num_bits: bools.len(),
        })
    }*/

    pub fn from_vec(bools: Vec<bool>) -> Result<Self, ArrayError> {
        let mut buffer = Buffer::new(Bitmap::num_of_bytes(bools.len()))?;
        let data = buffer.as_slice_mut();
        (0..bools.len()).for_each(|idx| {
            if bools[idx] {
                bit_util::set_bit(data, idx);
            } else {
                bit_util::unset_bit(data, idx);
            }
        });

        Ok(Self {
            bits: buffer,
            num_bits: bools.len(),
        })
    }

    pub fn num_of_bytes(num_bits: usize) -> usize {
        let num_bytes = num_bits / 8 + if num_bits % 8 > 0 { 1 } else { 0 };
        let r = num_bytes % 64;
        if r == 0 {
            num_bytes
        } else {
            num_bytes + 64 - r
        }
    }

    pub fn len(&self) -> usize {
        self.num_of_buffer_bytes() << 3
    }

    fn num_of_buffer_bytes(&self) -> usize {
        self.bits.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }

    pub unsafe fn is_set_unchecked(&self, idx: usize) -> bool {
        bit_util::get_bit_raw(self.bits.as_ptr(), idx)
    }

    pub fn is_set(&self, idx: usize) -> Result<bool, ArrayError> {
        self.check_idx(idx)?;

        // Justification
        // We've already checked index here, so it's ok to use unsafe.
        Ok(unsafe { self.is_set_unchecked(idx) })
    }

    pub fn buffer_ref(&self) -> &Buffer {
        &self.bits
    }

    pub fn into_buffer(self) -> Buffer {
        self.bits
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [Bitmap].
    pub fn get_buffer_memory_size(&self) -> usize {
        self.bits.capacity()
    }

    /// Returns the total number of bytes of memory occupied physically by this [Bitmap].
    pub fn get_array_memory_size(&self) -> usize {
        self.bits.capacity() + mem::size_of_val(self)
    }

    pub fn iter(&self) -> BitmapIter<'_> {
        BitmapIter {
            bits: &self.bits,
            idx: 0,
            num_bits: self.num_bits,
        }
    }

    fn check_idx(&self, idx: usize) -> Result<(), ArrayError> {
        if idx >= self.len() {
            Err(ArrayError::IndexOutOfBoundary)
        } else {
            Ok(())
        }
    }
}

impl<'a, 'b> BitAnd<&'b Bitmap> for &'a Bitmap {
    type Output = Result<Bitmap, ArrayError>;

    fn bitand(self, rhs: &'b Bitmap) -> Result<Bitmap, ArrayError> {
        Ok(Bitmap::from((&self.bits & &rhs.bits)?))
    }
}

impl<'a, 'b> BitOr<&'b Bitmap> for &'a Bitmap {
    type Output = Result<Bitmap, ArrayError>;

    fn bitor(self, rhs: &'b Bitmap) -> Result<Bitmap, ArrayError> {
        Ok(Bitmap::from((&self.bits | &rhs.bits)?))
    }
}

impl From<Buffer> for Bitmap {
    fn from(buf: Buffer) -> Self {
        Self {
            num_bits: buf.len() << 3,
            bits: buf,
        }
    }
}

impl PartialEq for Bitmap {
    fn eq(&self, other: &Self) -> bool {
        // buffer equality considers capacity, but here we want to only compare
        // actual data contents
        let self_len = self.bits.len();
        let other_len = other.bits.len();
        if self_len != other_len {
            return false;
        }
        self.bits.as_slice()[..self_len] == other.bits.as_slice()[..self_len]
    }
}

pub struct BitmapIter<'a> {
    bits: &'a Buffer,
    idx: usize,
    num_bits: usize,
}

impl<'a> BitmapIter<'a> {
    pub fn try_from(value: &'a Buffer, num_bits: usize) -> Result<Self, ArrayError> {
        if value.len() < Bitmap::num_of_bytes(num_bits) {
            Err(ArrayError::IndexOutOfBoundary)
        } else {
            Ok(Self {
                bits: value,
                idx: 0,
                num_bits,
            })
        }
    }
}

impl<'a> std::iter::Iterator for BitmapIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.num_bits {
            return None;
        }
        let b = unsafe { bit_util::get_bit_raw(self.bits.as_ptr(), self.idx) };
        self.idx += 1;
        Some(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_length() {
        assert_eq!(64, Bitmap::new(63 * 8).unwrap().num_of_buffer_bytes());
        assert_eq!(64, Bitmap::new(64 * 8).unwrap().num_of_buffer_bytes());
        assert_eq!(128, Bitmap::new(65 * 8).unwrap().num_of_buffer_bytes());
    }

    #[test]
    fn test_bitwise_and() {
        let bitmap1 = Bitmap::from(Buffer::try_from([0b01101010]).unwrap());
        let bitmap2 = Bitmap::from(Buffer::try_from([0b01001110]).unwrap());
        assert_eq!(
            Bitmap::from(Buffer::try_from([0b01001010]).unwrap()),
            (&bitmap1 & &bitmap2).unwrap()
        );
    }

    #[test]
    fn test_bitwise_or() {
        let bitmap1 = Bitmap::from(Buffer::try_from([0b01101010]).unwrap());
        let bitmap2 = Bitmap::from(Buffer::try_from([0b01001110]).unwrap());
        assert_eq!(
            Bitmap::from(Buffer::try_from([0b01101110]).unwrap()),
            (&bitmap1 | &bitmap2).unwrap()
        );
    }

    #[test]
    fn test_bitmap_is_set() {
        let bitmap = Bitmap::from(Buffer::try_from([0b01001010]).unwrap());
        assert!(!bitmap.is_set(0).unwrap());
        assert!(bitmap.is_set(1).unwrap());
        assert!(!bitmap.is_set(2).unwrap());
        assert!(bitmap.is_set(3).unwrap());
        assert!(!bitmap.is_set(4).unwrap());
        assert!(!bitmap.is_set(5).unwrap());
        assert!(bitmap.is_set(6).unwrap());
        assert!(!bitmap.is_set(7).unwrap());
    }

    #[test]
    fn test_bitmap_iter() -> Result<(), ArrayError> {
        {
            let bitmap = Bitmap::from(Buffer::try_from([0b01001010]).unwrap());
            let mut booleans = vec![];
            for b in bitmap.iter() {
                booleans.push(b as u8);
            }
            assert_eq!(booleans, vec![0u8, 1, 0, 1, 0, 0, 1, 0]);
        }
        {
            let mut booleans = vec![];
            for _ in 0..5 {
                booleans.push(true);
            }
            let bitmap = Bitmap::from_vec(booleans)?;
            for b in bitmap.iter() {
                assert_eq!(b, true);
            }
        }
        Ok(())
    }
}
