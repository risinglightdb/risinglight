// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::hash::{DefaultHasher, Hasher};

use super::*;

/// Distribute the input data to multiple partitions by hash partitioning.
pub struct HashPartitionProducer {
    /// The indices of the columns to hash.
    pub hash_key: Vec<usize>,
    /// The number of partitions.
    pub num_partitions: usize,
}

impl HashPartitionProducer {
    #[try_stream(boxed, ok = (DataChunk, usize), error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        // preallocate buffers for reuse
        let mut hashers = vec![DefaultHasher::default(); PROCESSING_WINDOW_SIZE];
        let mut partition_indices = vec![0; PROCESSING_WINDOW_SIZE];
        let mut visibility = vec![false; PROCESSING_WINDOW_SIZE];

        #[for_await]
        for batch in child {
            let batch = batch?;

            // reset buffers
            hashers.clear();
            hashers.resize(batch.cardinality(), DefaultHasher::default());
            partition_indices.resize(batch.cardinality(), 0);
            visibility.resize(batch.cardinality(), false);

            // calculate the hash
            for index in &self.hash_key {
                batch.array_at(*index).hash(&mut hashers);
            }
            for (hasher, target) in hashers.iter().zip(&mut partition_indices) {
                *target = hasher.finish() as usize % self.num_partitions;
            }

            // send the batch to the corresponding partition
            for partition in 0..self.num_partitions {
                for (row, p) in partition_indices.iter().enumerate() {
                    visibility[row] = *p == partition;
                }
                let chunk = batch.filter(&visibility);
                yield (chunk, partition);
            }
        }
    }
}

/// Randomly distribute the input data to multiple partitions.
pub struct RandomPartitionProducer {
    /// The number of partitions.
    pub num_partitions: usize,
}

impl RandomPartitionProducer {
    #[try_stream(boxed, ok = (DataChunk, usize), error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        todo!()
    }
}
