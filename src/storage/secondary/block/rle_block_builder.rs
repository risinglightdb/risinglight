// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;

use bytes::{Buf, BufMut};
use prost::DecodeError;
use risinglight_proto::rowset::BlockStatistics;

use super::BlockBuilder;
use crate::array::Array;

fn encode_32<B>(mut value: u32, buf: &mut B)
where
    B: BufMut,
{
    loop {
        if value < 0x80 {
            buf.put_u8(value as u8);
            break;
        } else {
            buf.put_u8(((value & 0x7F) | 0x80) as u8);
            value >>= 7;
        }
    }
}

fn decode_u32<B>(buf: &mut B) -> Result<Vec<u32>, DecodeError>
where
    B: Buf,
{
    // &mut
    let mut bytes = buf.chunk();
    let len = bytes.len();
    if len == 0 {
        return Err(DecodeError::new("invalid varint"));
    }
    let mut ret: Vec<u32> = Vec::new();
    let mut tot = 0;
    while tot < len {
        let (value, advance) = decode_u32_slice(bytes)?;
        ret.push(value);
        bytes.advance(advance);
        tot += advance;
    }
    Ok(ret)
}

// 使用inline加速, 注意一个mut 引用可以调用一个非mut引用的函数
#[inline]
fn decode_u32_slice(bytes: &[u8]) -> Result<(u32, usize), DecodeError> {
    assert!(!bytes.is_empty());
    let mut b: u8 = unsafe { *bytes.get_unchecked(0) };
    // u32 最多是5位
    let mut part0: u32 = u32::from(b);
    if b < 0x80 {
        return Ok((u32::from(part0), 1));
    };
    part0 -= 0x80;
    b = unsafe { *bytes.get_unchecked(1) };
    part0 += u32::from(b) << 7;
    if b < 0x80 {
        return Ok((u32::from(part0), 2));
    };
    part0 -= 0x80 << 7;
    b = unsafe { *bytes.get_unchecked(2) };
    part0 += u32::from(b) << 14;
    if b < 0x80 {
        return Ok((u32::from(part0), 3));
    };
    part0 -= 0x80 << 14;
    b = unsafe { *bytes.get_unchecked(3) };
    part0 += u32::from(b) << 21;
    if b < 0x80 {
        return Ok((u32::from(part0), 4));
    };
    part0 -= 0x80 << 21;
    b = unsafe { *bytes.get_unchecked(4) };
    if b < 0x0f {
        // or it will overflow
        return Ok((part0 + (u32::from(b) << 28), 5));
    };
    Err(DecodeError::new("invalid varint"))
}

/// Encodes fixed-width data into a block with run-length encoding. The layout is
/// rle counts and data from other block builder
/// ```plain
/// | rle_counts_num (u32) | rle_count (u16) | rle_count | data | data | (may be bit) |
/// ```
// rle == run-length encoding
pub struct RleBlockBuilder<A, B>
where
    A: Array,
    B: BlockBuilder<A>,
    A::Item: PartialEq,
{
    block_builder: B,
    // 其实要求的就是把rle_count压缩
    rle_counts: Vec<u32>,
    // rle_counts
    previous_value: Option<<A::Item as ToOwned>::Owned>,
    // previous_value A::Item
    cur_count: u32,
}

impl<A, B> RleBlockBuilder<A, B>
where
    A: Array,
    B: BlockBuilder<A>,
    A::Item: PartialEq, // 可以实现== 比较
{
    pub fn new(block_builder: B) -> Self {
        Self {
            block_builder,
            rle_counts: Vec::new(),
            previous_value: None,
            cur_count: 0,
        }
    }
}

impl<A, B> BlockBuilder<A> for RleBlockBuilder<A, B>
where
    A: Array,
    B: BlockBuilder<A>,
    A::Item: PartialEq,
{
    fn append(&mut self, item: Option<&A::Item>) {
        if self.cur_count == 0 {
            // only happens for the very first append
            self.previous_value = item.map(|x| x.to_owned());
            self.block_builder.append(item);
            self.cur_count = 1;
            return;
        }
        // x.to_owned() ==> create owned data from borrowed data, usually by cloning
        // as_ref() ==> converts from &Option<T> to Option<&T>

        // 我们可以这样理解, rust里面的reference 相当于是一个指针，也是一个类型, 所以其实也可以
        // 视作有所有权
        // 注意rust里面有四个概念需要区分一下
        // map因为调用的参数是self, 所以会消耗自己
        // 使用as_ref可以在不消耗自己的
        // u32::MAX
        if item != self.previous_value.as_ref().map(|x| x.borrow()) || self.cur_count == u32::MAX {
            self.previous_value = item.map(|x| x.to_owned());
            self.block_builder.append(item);
            self.rle_counts.push(self.cur_count);
            self.cur_count = 1;
        } else {
            self.cur_count += 1;
        }
    }

    fn estimated_size(&self) -> usize {
        self.block_builder.estimated_size()
            + self.rle_counts.len() * std::mem::size_of::<u16>()
            + std::mem::size_of::<u32>()
            + (self.cur_count != 0) as usize * std::mem::size_of::<u16>()
    }

    fn should_finish(&self, next_item: &Option<&A::Item>) -> bool {
        self.block_builder.should_finish(next_item)
    }

    fn get_statistics(&self) -> Vec<BlockStatistics> {
        self.block_builder.get_statistics()
    }

    fn finish(mut self) -> Vec<u8> {
        let mut encoded_data: Vec<u8> = vec![];
        if self.cur_count == 0 {
            // No data at all
            return encoded_data;
        }
        self.rle_counts.push(self.cur_count);

        // 几个关键点, as 关键字可以实现类型强转
        encoded_data.put_u32_le(self.rle_counts.len() as u32);

        // 关键在于count这个地方可以改成把每一个count 通过variable encoding
        for count in self.rle_counts {
            encode_32(count, &mut encoded_data);
        }
        let data = self.block_builder.finish();
        encoded_data.extend(data);
        encoded_data
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::super::{
        PlainBlobBlockBuilder, PlainCharBlockBuilder, PlainPrimitiveBlockBuilder,
        PlainPrimitiveNullableBlockBuilder,
    };
    use super::*;
    use crate::array::{I32Array, Utf8Array};

    #[test]
    fn test_build_rle_primitive_i32() {
        // Test primitive rle block builder for i32
        let builder = PlainPrimitiveBlockBuilder::new(14);
        let mut rle_builder =
            RleBlockBuilder::<I32Array, PlainPrimitiveBlockBuilder<i32>>::new(builder);
        for item in [Some(&1)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&2)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&3)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        assert_eq!(rle_builder.estimated_size(), 4 * 3 + 2 * 3 + 4);
        assert!(rle_builder.should_finish(&Some(&3)));
        assert!(rle_builder.should_finish(&Some(&4)));
        rle_builder.finish();
    }

    #[test]
    fn test_build_rle_primitive_nullable_i32() {
        // Test primitive nullable rle block builder for i32
        let builder = PlainPrimitiveNullableBlockBuilder::new(48);
        let mut rle_builder =
            RleBlockBuilder::<I32Array, PlainPrimitiveNullableBlockBuilder<i32>>::new(builder);
        for item in [None].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&1)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&2)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&3)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&4)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&5)]
            .iter()
            .cycle()
            .cloned()
            .take(u16::MAX as usize * 2)
        {
            rle_builder.append(item);
        }
        assert_eq!(rle_builder.estimated_size(), 11 * 4 + 2 + 11 * 2 + 4);
        assert!(rle_builder.should_finish(&Some(&5)));
        rle_builder.finish();
    }

    #[test]
    fn test_build_rle_char() {
        // Test rle block builder for char
        let builder = PlainCharBlockBuilder::new(120, 40);
        let mut rle_builder = RleBlockBuilder::<Utf8Array, PlainCharBlockBuilder>::new(builder);

        let width_40_char = ["2"].iter().cycle().take(40).join("");

        for item in [Some("233")].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some("2333")].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&width_40_char[..])].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        assert_eq!(rle_builder.estimated_size(), 40 * 3 + 2 * 3 + 4);
        assert!(rle_builder.should_finish(&Some(&width_40_char[..])));
        // should_finish is not very accurate
        assert!(rle_builder.should_finish(&Some("2333333")));
        rle_builder.finish();
    }

    #[test]
    fn test_build_rle_varchar() {
        // Test rle block builder for varchar
        let builder = PlainBlobBlockBuilder::new(30);
        let mut rle_builder =
            RleBlockBuilder::<Utf8Array, PlainBlobBlockBuilder<str>>::new(builder);
        for item in [Some("233")].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some("23333")].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some("2333333")].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        assert_eq!(rle_builder.estimated_size(), 15 + 4 * 3 + 2 * 3 + 4); // 37
        assert!(rle_builder.should_finish(&Some("2333333")));
        // should_finish is not very accurate
        assert!(rle_builder.should_finish(&Some("23333333")));
        rle_builder.finish();
    }
}
