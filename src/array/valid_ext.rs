use bitvec::vec::BitVec;

pub trait ArrayValidExt {
    fn get_valid_bitmap(&self) -> &BitVec;
}
