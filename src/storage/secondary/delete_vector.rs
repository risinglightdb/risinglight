// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use bitvec::prelude::BitVec;
use futures::pin_mut;
use itertools::Itertools;
use prost::Message;
use risinglight_proto::rowset::DeleteRecord;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

use crate::storage::StorageResult;

pub struct DeleteVector {
    dv_id: u64,
    rowset_id: u32,
    deletes: Vec<u32>,
}

impl DeleteVector {
    pub async fn open(dv_id: u64, rowset_id: u32, path: impl AsRef<Path>) -> StorageResult<Self> {
        let mut reader = BufReader::new(tokio::fs::File::open(path).await?);
        let mut data = Vec::new();

        // TODO: don't read all to memory
        reader.read_to_end(&mut data).await?;

        let mut buf = &data[..];
        let mut deletes = vec![];

        while !buf.is_empty() {
            deletes.push(DeleteRecord::decode_length_delimited(&mut buf)?.row_id);
        }

        deletes.sort_unstable();
        deletes.dedup();

        Ok(Self {
            dv_id,
            rowset_id,
            deletes,
        })
    }

    pub async fn write_all(
        file: impl tokio::io::AsyncWrite,
        deletes: &[DeleteRecord],
    ) -> StorageResult<()> {
        pin_mut!(file);
        let mut data = Vec::new();
        for delete in deletes {
            delete.encode_length_delimited(&mut data)?;
        }
        file.write_all(&data).await?;
        Ok(())
    }

    pub fn new(dv_id: u64, rowset_id: u32, deletes: Vec<DeleteRecord>) -> Self {
        let mut deletes = deletes.into_iter().map(|x| x.row_id).collect_vec();
        deletes.sort_unstable();
        deletes.dedup();

        Self {
            dv_id,
            rowset_id,
            deletes,
        }
    }

    pub fn dv_id(&self) -> u64 {
        self.dv_id
    }

    pub fn rowset_id(&self) -> u32 {
        self.rowset_id
    }

    /// Apply the current DV info to a visibility bitmap
    pub fn apply_to(&self, data: &mut BitVec, offset_row_id: u32) {
        let pos = self.deletes.partition_point(|x| *x < offset_row_id);

        let mut iter = self.deletes.iter().skip(pos).peekable();

        for (row_id, mut bitref) in (offset_row_id as usize..).zip(data.iter_mut()) {
            if let Some(unset_row_id) = iter.peek() {
                if **unset_row_id == row_id as u32 {
                    bitref.set(false);
                    iter.next();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bitvec::prelude::*;

    use super::*;

    #[test]
    fn test_dv_apply() {
        let dv = DeleteVector::new(
            0,
            0,
            vec![DeleteRecord { row_id: 3 }, DeleteRecord { row_id: 5 }],
        );
        let mut bv = BitVec::new();
        bv.resize(3, true);
        dv.apply_to(&mut bv, 0);
        assert_eq!(bv, bitvec![1, 1, 1]);

        let mut bv = BitVec::new();
        bv.resize(3, true);
        dv.apply_to(&mut bv, 1);
        assert_eq!(bv, bitvec![1, 1, 0]);

        let mut bv = BitVec::new();
        bv.resize(3, true);
        dv.apply_to(&mut bv, 3);
        assert_eq!(bv, bitvec![0, 1, 0]);

        let mut bv = BitVec::new();
        bv.resize(3, true);
        dv.apply_to(&mut bv, 4);
        assert_eq!(bv, bitvec![1, 0, 1]);
    }
}
