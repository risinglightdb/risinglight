mod block;
mod disk_manager;
mod segment;
mod slice;
mod table;

// On-disk Storage
// Each table with N columns is stored in multiple table slices.
// (For our stand-alone system, we only store table in one slice. We could make the storage shared in distributed system.)
// Each slice has mutiple table segments.
// Each segment have N column segments.
// Each column segment store data in a list of Block.
pub const BLOCK_SIZE: usize = 2 * 1024 * 1024;
pub type BlockId = u32;
pub type TableSegmentId = u32;
pub type SliceId = u32;
pub type TupleSize = u64;
pub type SegmentSize = u64;
