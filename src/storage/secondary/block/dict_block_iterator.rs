// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::marker::PhantomData;

use bytes::Buf;

use super::{Block, BlockIterator, PlainPrimitiveBlockIterator, RleBlockIterator};
use crate::array::{Array, ArrayBuilder, I32Array, I32ArrayBuilder};
use crate::storage::secondary::block::dict_block_builder::DICT_NULL_VALUE_KEY;

pub fn decode_dict_block(data: Block) -> (usize, Block, Block) {
    let mut buffer = &data[..];
    let rle_length = buffer.get_u64() as usize;
    let dict_count_sum = buffer.get_u32() as usize;
    let constant_length = std::mem::size_of::<u32>() + std::mem::size_of::<u64>();
    let rle_block = data.slice(constant_length..(rle_length + constant_length));
    let dict_block = data.slice((rle_length + constant_length)..);
    (dict_count_sum, dict_block, rle_block)
}

/// Scans one or several arrays from the RLE Primitive block content,
/// including plain block and nullable block.
pub struct DictBlockIterator<A, B>
where
    A: Array,
    B: BlockIterator<A>,
{
    rle_iter: RleBlockIterator<I32Array, PlainPrimitiveBlockIterator<i32>>,

    dict: HashMap<i32, <A::Item as ToOwned>::Owned>,

    phantom_data: PhantomData<B>,
}

impl<A, B> DictBlockIterator<A, B>
where
    A: Array,
    B: BlockIterator<A>,
{
    pub fn new(
        dict_builder: &mut A::Builder,
        dict_iter: &mut B,
        rle_block: Block,
        dict_num: usize,
    ) -> Self {
        dict_iter.next_batch(Some(dict_num), dict_builder);
        let items = <A::Builder as ArrayBuilder>::take(dict_builder);
        assert_eq!(
            items.len(),
            dict_num,
            "Check if dict_builder and dict_num match"
        );
        let mut code = DICT_NULL_VALUE_KEY + 1;
        let mut dict = HashMap::new();
        let (rle_num, rle_data, block_data) = super::decode_rle_block(rle_block);
        for item in items.iter().map(|item| (item.unwrap().to_owned())) {
            dict.insert(code, item);
            code += 1;
        }
        let block_iter = PlainPrimitiveBlockIterator::new(block_data, rle_num);
        let rle_iter = RleBlockIterator::<I32Array, PlainPrimitiveBlockIterator<i32>>::new(
            block_iter, rle_data, rle_num,
        );
        Self {
            rle_iter,
            dict,
            phantom_data: PhantomData,
        }
    }
}

impl<A, B> BlockIterator<A> for DictBlockIterator<A, B>
where
    A: Array,
    B: BlockIterator<A>,
{
    fn next_batch(&mut self, expected_size: Option<usize>, dict_builder: &mut A::Builder) -> usize {
        let mut builder = I32ArrayBuilder::new();
        let size = self.rle_iter.next_batch(expected_size, &mut builder);
        let rle_code = builder.finish();
        for code in rle_code.iter() {
            match code {
                Some(code) => {
                    if code.eq(&DICT_NULL_VALUE_KEY) {
                        dict_builder.push(None);
                    } else {
                        let value = self.dict.get(code);
                        dict_builder.push(value.map(|x| x.borrow()));
                    }
                }
                None => panic!("dict block has been damaged"),
            }
        }
        size
    }

    fn skip(&mut self, cnt: usize) {
        self.rle_iter.skip(cnt);
    }

    fn remaining_items(&self) -> usize {
        self.rle_iter.remaining_items()
    }
}
#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::Itertools;

    use crate::array::{
        ArrayBuilder, ArrayToVecExt, BlobArray, BlobArrayBuilder, I32Array, I32ArrayBuilder,
        Utf8Array, Utf8ArrayBuilder,
    };
    use crate::storage::secondary::block::dict_block_builder::DictBlockBuilder;
    use crate::storage::secondary::block::dict_block_iterator::{
        decode_dict_block, DictBlockIterator,
    };
    use crate::storage::secondary::block::{
        decode_nullable_block, BlockBuilder, BlockIterator, NullableBlockBuilder,
        NullableBlockIterator, PlainBlobBlockBuilder, PlainBlobBlockIterator,
        PlainCharBlockBuilder, PlainCharBlockIterator, PlainPrimitiveBlockBuilder,
        PlainPrimitiveBlockIterator,
    };
    use crate::types::{Blob, BlobRef};

    #[test]
    fn test_scan_dict_i32() {
        let builder = PlainPrimitiveBlockBuilder::<i32>::new(20);
        let mut dict_builder = DictBlockBuilder::new(builder);
        for item in [Some(&1)].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [Some(&2)].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [Some(&3)].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        let data = dict_builder.finish();

        let (dict_num, block_data, rle_data) = decode_dict_block(Bytes::from(data));
        let mut dict_builder = I32ArrayBuilder::new();
        let mut dict_iter = PlainPrimitiveBlockIterator::new(block_data, dict_num);
        let mut scanner = DictBlockIterator::<I32Array, PlainPrimitiveBlockIterator<i32>>::new(
            &mut dict_builder,
            &mut dict_iter,
            rle_data,
            dict_num,
        );

        let mut builder = I32ArrayBuilder::new();

        assert_eq!(scanner.next_batch(Some(1), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![Some(1)]);

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 5);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(builder.finish().to_vec(), vec![Some(2), Some(2)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);

        assert_eq!(builder.finish().to_vec(), vec![Some(3), Some(3)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(3), &mut builder), 1);

        assert_eq!(builder.finish().to_vec(), vec![Some(3)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }

    #[test]
    fn test_scan_dict_nullable_i32() {
        let inner_builder = PlainPrimitiveBlockBuilder::<i32>::new(50);
        let builder = NullableBlockBuilder::new(inner_builder, 50);
        let mut dict_builder = DictBlockBuilder::new(builder);
        for item in [None].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [Some(&1)].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [Some(&2)].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [Some(&3)].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        let data = dict_builder.finish();

        let (dict_num, block_data, rle_data) = decode_dict_block(Bytes::from(data));
        let (inner_block, bitmap_block) = decode_nullable_block(block_data);
        let inner_iter = PlainPrimitiveBlockIterator::<i32>::new(inner_block, dict_num);
        let mut dict_iter = NullableBlockIterator::new(inner_iter, bitmap_block);
        let mut scanner = DictBlockIterator::new(
            &mut I32ArrayBuilder::new(),
            &mut dict_iter,
            rle_data,
            dict_num,
        );

        let mut builder = I32ArrayBuilder::new();

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 15);

        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(builder.finish().to_vec(), vec![Some(1), Some(1)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(6), &mut builder), 6);

        assert_eq!(
            builder.finish().to_vec(),
            vec![Some(1), None, None, None, Some(2), Some(2)]
        );

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(8), &mut builder), 7);

        assert_eq!(
            builder.finish().to_vec(),
            vec![Some(2), None, None, None, Some(3), Some(3), Some(3)]
        );

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }

    #[test]
    fn test_scan_dict_char() {
        let builder = PlainCharBlockBuilder::new(120, 40);
        let mut dict_builder = DictBlockBuilder::<Utf8Array, PlainCharBlockBuilder>::new(builder);

        let width_40_char = ["2"].iter().cycle().take(40).join("");

        for item in [Some("233")].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [Some("2333")].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [Some(&width_40_char[..])].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        let data = dict_builder.finish();

        let (dict_num, block_data, rle_data) = decode_dict_block(Bytes::from(data));
        let mut dict_builder = Utf8ArrayBuilder::new();
        let mut dict_iter = PlainCharBlockIterator::new(block_data, dict_num, 40);
        let mut scanner = DictBlockIterator::<Utf8Array, PlainCharBlockIterator>::new(
            &mut dict_builder,
            &mut dict_iter,
            rle_data,
            dict_num,
        );
        let mut builder = Utf8ArrayBuilder::new();

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 6);

        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(
            builder.finish().to_vec(),
            vec![Some("2333".to_string()), Some("2333".to_string())]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);

        assert_eq!(
            builder.finish().to_vec(),
            vec![Some("2333".to_string()), Some(width_40_char.clone())]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(3), &mut builder), 2);

        assert_eq!(
            builder.finish().to_vec(),
            vec![Some(width_40_char.clone()), Some(width_40_char.clone())]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }

    #[test]
    fn test_scan_dict_varchar() {
        let builder = PlainBlobBlockBuilder::new(30);
        let mut dict_builder =
            DictBlockBuilder::<Utf8Array, PlainBlobBlockBuilder<str>>::new(builder);
        for item in [Some("233")].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [Some("23333")].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [Some("2333333")].iter().cycle().cloned().take(2) {
            dict_builder.append(item);
        }
        let data = dict_builder.finish();

        let (dict_num, block_data, rle_data) = decode_dict_block(Bytes::from(data));
        let mut dict_builder = Utf8ArrayBuilder::new();
        let mut dict_iter = PlainBlobBlockIterator::new(block_data, dict_num);
        let mut scanner = DictBlockIterator::<Utf8Array, PlainBlobBlockIterator<str>>::new(
            &mut dict_builder,
            &mut dict_iter,
            rle_data,
            dict_num,
        );
        let mut builder = Utf8ArrayBuilder::new();

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 5);

        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(
            builder.finish().to_vec(),
            vec![Some("23333".to_string()), Some("23333".to_string())]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(6), &mut builder), 3);

        assert_eq!(
            builder.finish().to_vec(),
            vec![
                Some("23333".to_string()),
                Some("2333333".to_string()),
                Some("2333333".to_string())
            ]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }

    #[test]
    fn test_scan_rle_blob() {
        let builder = PlainBlobBlockBuilder::new(30);
        let mut dict_builder =
            DictBlockBuilder::<BlobArray, PlainBlobBlockBuilder<BlobRef>>::new(builder);
        for item in [Some(BlobRef::new("233".as_bytes()))]
            .iter()
            .cycle()
            .cloned()
            .take(3)
        {
            dict_builder.append(item);
        }
        for item in [Some(BlobRef::new("23333".as_bytes()))]
            .iter()
            .cycle()
            .cloned()
            .take(3)
        {
            dict_builder.append(item);
        }
        for item in [Some(BlobRef::new("2333333".as_bytes()))]
            .iter()
            .cycle()
            .cloned()
            .take(2)
        {
            dict_builder.append(item);
        }
        let data = dict_builder.finish();

        let (dict_num, block_data, rle_data) = decode_dict_block(Bytes::from(data));
        let mut dict_builder = BlobArrayBuilder::new();
        let mut dict_iter = PlainBlobBlockIterator::new(block_data, dict_num);
        let mut scanner = DictBlockIterator::<BlobArray, PlainBlobBlockIterator<BlobRef>>::new(
            &mut dict_builder,
            &mut dict_iter,
            rle_data,
            dict_num,
        );

        let mut builder = BlobArrayBuilder::new();

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 5);

        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(
            builder.finish().to_vec(),
            vec![
                Some(Blob::from("23333".as_bytes())),
                Some(Blob::from("23333".as_bytes()))
            ]
        );

        let mut builder = BlobArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(6), &mut builder), 3);

        assert_eq!(
            builder.finish().to_vec(),
            vec![
                Some(Blob::from("23333".as_bytes())),
                Some(Blob::from("2333333".as_bytes())),
                Some(Blob::from("2333333".as_bytes()))
            ]
        );

        let mut builder = BlobArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }

    #[test]
    fn test_scan_dict_skip() {
        let inner_builder = PlainPrimitiveBlockBuilder::<i32>::new(50);
        let builder = NullableBlockBuilder::new(inner_builder, 50);
        let mut dict_builder = DictBlockBuilder::new(builder);
        for item in [None].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [Some(&1)].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [Some(&2)].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        for item in [Some(&3)].iter().cycle().cloned().take(3) {
            dict_builder.append(item);
        }
        let data = dict_builder.finish();

        let (dict_num, block_data, rle_data) = decode_dict_block(Bytes::from(data));
        let (inner_block, bitmap_block) = decode_nullable_block(block_data);
        let inner_iter = PlainPrimitiveBlockIterator::<i32>::new(inner_block, dict_num);
        let mut dict_iter = NullableBlockIterator::new(inner_iter, bitmap_block);
        let mut scanner = DictBlockIterator::new(
            &mut I32ArrayBuilder::new(),
            &mut dict_iter,
            rle_data,
            dict_num,
        );

        let mut builder = I32ArrayBuilder::new();

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 15);

        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(builder.finish().to_vec(), vec![Some(1), Some(1)]);

        scanner.skip(8);
        assert_eq!(scanner.remaining_items(), 5);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(3), &mut builder), 3);

        assert_eq!(builder.finish().to_vec(), vec![None, None, Some(3)]);

        scanner.skip(1);
        assert_eq!(scanner.remaining_items(), 1);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(3), &mut builder), 1);

        assert_eq!(builder.finish().to_vec(), vec![Some(3)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }
}
