// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use super::{SecondaryIterator, SecondaryIteratorImpl};
use crate::array::{ArrayBuilderImpl, ArrayImplBuilderPickExt};
use crate::storage::{PackedVec, StorageChunk, StorageResult};

/// [`MergeIterator`] merges data from multiple sorted `RowSet`s.
/// This iterator should be used on sorted mode with overlapping sort keys.
pub struct MergeIterator {
    /// All child iterators
    ///
    /// TODO: should be able to accept `ConcatIterator` as parameter.
    iters: Vec<SecondaryIterator>,

    /// Buffer of data chunks from each iterator
    chunk_buffer: Vec<Option<StorageChunk>>,

    /// Indicates whether an iterator has reached its end
    has_finished: Vec<bool>,

    /// The column id to be used as sort key
    sort_key_idx: Vec<usize>,

    /// The min-heap of all pending data. Heap will be no larger than
    /// `len(iters)` size. Each element represents `(iter_id, batch_row_id)`.
    /// As we have to implement a lot of custom compare logic, we have
    /// to implement our own binary heap.
    pending_heap: Vec<(usize, usize)>,
}

impl MergeIterator {
    pub fn new(iters: Vec<SecondaryIterator>, sort_key_idx: Vec<usize>) -> Self {
        Self {
            sort_key_idx,
            chunk_buffer: vec![None; iters.len()],
            has_finished: vec![false; iters.len()],
            iters,
            pending_heap: vec![],
        }
    }

    fn compare_data(
        &self,
        (left_id, left_batch_row_id): &(usize, usize),
        (right_id, right_batch_row_id): &(usize, usize),
    ) -> std::cmp::Ordering {
        let left_chunk = self.chunk_buffer[*left_id].as_ref().unwrap();
        let right_chunk = self.chunk_buffer[*right_id].as_ref().unwrap();

        for sort_key_idx in &self.sort_key_idx {
            let left_data = left_chunk.array_at(*sort_key_idx).get(*left_batch_row_id);
            let right_data = right_chunk.array_at(*sort_key_idx).get(*right_batch_row_id);
            // TODO: handle can-not-compare
            let res = left_data.partial_cmp(&right_data).unwrap();
            if Ordering::Equal != res {
                return res;
            }
        }
        Ordering::Equal
    }

    fn compare_in_heap(&self, left_idx: usize, right_idx: usize) -> std::cmp::Ordering {
        self.compare_data(&self.pending_heap[left_idx], &self.pending_heap[right_idx])
    }

    /// Add a (rowset, row) to the pending data heap
    fn add_pending_data(&mut self, (iter_id, batch_row_id): (usize, usize)) {
        // add the element to the end of the heap
        self.pending_heap.push((iter_id, batch_row_id));
        let mut processing_element = self.pending_heap.len() - 1;
        while processing_element > 0 {
            // id of each element in heap:
            //
            //    0
            //  1   2
            // 3 4 5 6
            //
            // parent = (child - 1) / 2
            let parent_element = (processing_element - 1) / 2;

            let compare_result = self.compare_in_heap(parent_element, processing_element);

            // As we are maintaining a min-heap, if the parent element is greater than the current
            // one, push that down.
            if matches!(compare_result, std::cmp::Ordering::Greater) {
                self.pending_heap.swap(parent_element, processing_element);
                processing_element = parent_element;
            } else {
                break;
            }
        }
    }

    /// Peek the top-most element
    fn peek_pending_data(&self) -> (usize, usize) {
        self.pending_heap[0]
    }

    /// Pop an element and insert a new element.
    ///
    /// This is an optimization on the heap. If the added element is less than both of the
    /// root's children, it can directly replace the root element without going through
    /// the whole normal insert process. For example, assume the following situation:
    ///
    /// ```plain
    ///      233
    ///  1000    1002
    /// a    b  c    d
    /// ```
    ///
    /// And we want to insert 234 into the heap and pop 233. As 234 is less than both 1000
    /// and 1002, we can directly replace 233 with 234.
    ///
    /// This would be a common case if the RowSets are not overlapping too much. We can
    /// continuously read from a single iterator.
    fn replace_pending_data(&mut self, (iter_id, batch_row_id): (usize, usize)) -> (usize, usize) {
        let pop_data = std::mem::replace(&mut self.pending_heap[0], (iter_id, batch_row_id));
        let mut processing_element = 0;
        loop {
            let left_child = processing_element * 2 + 1;
            if left_child >= self.pending_heap.len() {
                // no children left
                break;
            }
            let right_child = processing_element * 2 + 2;

            // The child to swap with the processing element, which is the minimum among left and
            // right child.
            let mut selected_child = left_child;
            if right_child < self.pending_data_len()
                && matches!(
                    self.compare_in_heap(left_child, right_child),
                    std::cmp::Ordering::Greater
                )
            {
                selected_child = right_child;
            }

            // Check if we need to push down
            if matches!(
                self.compare_in_heap(processing_element, selected_child),
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal
            ) {
                break;
            }

            self.pending_heap.swap(processing_element, selected_child);
            processing_element = selected_child;
        }
        pop_data
    }

    /// Pop an element from the heap
    fn pop_pending_data(&mut self) -> (usize, usize) {
        // move the last element to the root
        let last_element = self.pending_heap.pop().unwrap();
        if self.pending_data_len() > 0 {
            self.replace_pending_data(last_element)
        } else {
            last_element
        }
    }

    fn pending_data_len(&self) -> usize {
        self.pending_heap.len()
    }

    /// Fill the `chunk_buffer` with data from `iter_idx` iterator. If successful, return `true`.
    async fn request_fill_buffer(
        &mut self,
        iter_idx: usize,
        expected_size: Option<usize>,
    ) -> StorageResult<bool> {
        if !self.has_finished[iter_idx] {
            if let Some(batch) = self.iters[iter_idx].next_batch(expected_size).await? {
                self.chunk_buffer[iter_idx] = Some(batch);
                return Ok(true);
            } else {
                self.has_finished[iter_idx] = true;
            }
        }

        Ok(false)
    }

    /// Find the next visible item from chunk buffer of `iter_idx`. When `last_idx` is `None`,
    /// start from the first element in the visibility map. Otherwise, start from `last_idx + 1`.
    /// If there are no more visible elements, return `None`.
    fn next_visible_item(&self, iter_idx: usize, last_idx: Option<usize>) -> Option<usize> {
        let first_scan_idx = last_idx.map(|x| x + 1).unwrap_or(0);
        let chunk = self.chunk_buffer[iter_idx].as_ref().unwrap();
        let visibility = chunk.visibility();
        match visibility {
            Some(visibility) => {
                for idx in first_scan_idx..visibility.len() {
                    if visibility[idx] {
                        return Some(idx);
                    }
                }
                None
            }
            None => {
                if first_scan_idx >= chunk.row_count() {
                    None
                } else {
                    Some(first_scan_idx)
                }
            }
        }
    }

    /// Fetch a batch from child iterators and merge them. A batch will be returned when:
    ///
    /// * `expected_size` number of entries are fetched.
    /// * Any batch from any single iterator has been consumed.
    ///
    /// Therefore, except for the first call, each `next_batch` will incur at most one I/O
    /// (if no row is deleted) to the child iterators.
    pub async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<StorageChunk>> {
        // We must use one of the chunk buffer to construct builders
        let mut reference_chunk_buffer = None;

        for idx in 0..self.iters.len() {
            if self.chunk_buffer[idx].is_none() {
                if self.request_fill_buffer(idx, expected_size).await? {
                    let next_item = self.next_visible_item(idx, None).unwrap();
                    self.add_pending_data((idx, next_item));
                    reference_chunk_buffer = Some(idx);
                }
            } else {
                reference_chunk_buffer = Some(idx);
            }
        }

        if self.pending_data_len() == 0 {
            return Ok(None);
        }

        let mut pick_from = vec![];
        let mut reset_chunk_idx = None;
        let reference_chunk_buffer = reference_chunk_buffer.unwrap();

        loop {
            if let Some(expected_size) = expected_size {
                if pick_from.len() >= expected_size {
                    break;
                }
            }
            let (iter_idx, last_idx) = self.peek_pending_data();
            if let Some(next_idx) = self.next_visible_item(iter_idx, Some(last_idx)) {
                pick_from.push(self.replace_pending_data((iter_idx, next_idx)));
            } else {
                pick_from.push(self.pop_pending_data());
                reset_chunk_idx = Some(iter_idx);
                break;
            }
        }

        let builders = self.chunk_buffer[reference_chunk_buffer]
            .as_ref()
            .unwrap()
            .arrays()
            .iter()
            .map(ArrayBuilderImpl::from_type_of_array);

        let arrays = builders
            .enumerate()
            .map(|(col_idx, mut builder)| {
                let empty_array = builder.take();
                // ensure the array is empty and we didn't accidentally consume something.
                debug_assert!(empty_array.is_empty());

                let arrays = self
                    .chunk_buffer
                    .iter()
                    .map(|chunk| {
                        chunk
                            .as_ref()
                            .map(|x| x.array_at(col_idx))
                            .unwrap_or(&empty_array)
                            .clone()
                    })
                    .collect::<PackedVec<_>>();

                builder.pick_from_multiple(&arrays, &pick_from);
                builder.finish()
            })
            .collect::<PackedVec<_>>();

        if let Some(reset_chunk_idx) = reset_chunk_idx {
            // Remove buffer of no items left
            self.chunk_buffer[reset_chunk_idx] = None;
        }

        Ok(Some(StorageChunk::construct(None, arrays).unwrap()))
    }
}

impl SecondaryIteratorImpl for MergeIterator {}

#[cfg(test)]
mod tests {
    use bitvec::prelude::{BitVec, *};
    use itertools::Itertools;
    use smallvec::{smallvec, SmallVec};

    use super::*;
    use crate::array::{ArrayImpl, ArrayToVecExt, I32Array};
    use crate::storage::secondary::tests::TestIterator;

    pub fn array_to_chunk(
        visibility: Option<BitVec>,
        arrays: SmallVec<[ArrayImpl; 16]>,
    ) -> StorageChunk {
        StorageChunk::construct(visibility, arrays).expect("failed to construct StorageChunk")
    }

    #[tokio::test]
    async fn test_merge_iterator_one_iter() {
        let iter1 = TestIterator::new(vec![
            array_to_chunk(
                None,
                smallvec![I32Array::from_iter([1, 2, 3].map(Some)).into()],
            ),
            array_to_chunk(
                Some(bitvec![0, 0, 1]),
                smallvec![I32Array::from_iter([4, 5, 6].map(Some)).into()],
            ),
        ]);
        let mut merge_iterator = MergeIterator::new(vec![iter1.into()], vec![0]);
        let batch = merge_iterator.next_batch(Some(1)).await.unwrap().unwrap();
        let array: &I32Array = batch.array_at(0).try_into().unwrap();
        assert_eq!(array.to_vec(), vec![Some(1)]);
        let batch = merge_iterator.next_batch(Some(2)).await.unwrap().unwrap();
        let array: &I32Array = batch.array_at(0).try_into().unwrap();
        assert_eq!(array.to_vec(), vec![Some(2), Some(3)]);
        let batch = merge_iterator.next_batch(None).await.unwrap().unwrap();
        let array: &I32Array = batch.array_at(0).try_into().unwrap();
        assert_eq!(array.to_vec(), vec![Some(6)]);
    }

    #[tokio::test]
    async fn test_merge_iterator_two_iter() {
        let iter1 = TestIterator::new(vec![
            array_to_chunk(
                None,
                smallvec![I32Array::from_iter([1, 2, 4, 6].map(Some)).into()],
            ),
            array_to_chunk(
                Some(bitvec![0, 0, 1]),
                smallvec![I32Array::from_iter([5, 5, 6].map(Some)).into()],
            ),
        ]);
        let iter2 = TestIterator::new(vec![
            array_to_chunk(
                None,
                smallvec![I32Array::from_iter([3, 4, 5].map(Some)).into()],
            ),
            array_to_chunk(
                Some(bitvec![1, 0, 1]),
                smallvec![I32Array::from_iter([7, 8, 9].map(Some)).into()],
            ),
        ]);
        let mut merge_iterator = MergeIterator::new(vec![iter1.into(), iter2.into()], vec![0]);
        let answers = vec![vec![1, 2, 3, 4, 4, 5], vec![6], vec![6], vec![7, 9]];
        for answer in answers {
            let batch = merge_iterator.next_batch(None).await.unwrap().unwrap();
            let array: &I32Array = batch.array_at(0).try_into().unwrap();
            assert_eq!(array.to_vec(), answer.into_iter().map(Some).collect_vec());
        }
    }

    #[tokio::test]
    async fn test_merge_iterator_two_iter_composite() {
        let iter1 = TestIterator::new(vec![array_to_chunk(
            None,
            smallvec![
                I32Array::from_iter([1, 2, 3].map(Some)).into(),
                I32Array::from_iter([1, 3, 1].map(Some)).into()
            ],
        )]);

        let iter2 = TestIterator::new(vec![array_to_chunk(
            None,
            smallvec![
                I32Array::from_iter([1, 2, 2].map(Some)).into(),
                I32Array::from_iter([7, 1, 2].map(Some)).into()
            ],
        )]);

        let mut merge_iterator = MergeIterator::new(vec![iter1.into(), iter2.into()], vec![0, 1]);
        let answers = vec![
            vec![vec![1, 1, 2, 2], vec![1, 7, 1, 2]],
            vec![vec![2, 3], vec![3, 1]],
        ];
        for answer in answers {
            let batch = merge_iterator.next_batch(None).await.unwrap().unwrap();
            for (idx, item) in answer.iter().enumerate() {
                let answer_cmp = (*item).clone();
                let array: &I32Array = batch.array_at(idx).try_into().unwrap();
                assert_eq!(
                    array.to_vec(),
                    answer_cmp.into_iter().map(Some).collect_vec()
                );
            }
        }
    }
}
