use bitvec::vec::BitVec;

use super::Array;

pub trait ArrayValidExt: Array {
    fn get_valid_bitmap(&self) -> &BitVec;
}
