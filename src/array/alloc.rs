// Author: Renjie Liu (liurenjie2008@gmail.com)
use super::*;
use std::alloc::alloc;
use std::alloc::dealloc;
use std::alloc::Layout;
use std::ptr::NonNull;

const ALIGNMENT: usize = 1 << 6;

pub fn alloc_aligned(size: usize) -> Result<NonNull<u8>, ArrayError> {
    let layout = unsafe { Layout::from_size_align_unchecked(size, ALIGNMENT) };

    let ptr = unsafe { alloc(layout) };
    NonNull::new(ptr).ok_or_else(|| ArrayError::MemoryError)
}

pub fn free_aligned(size: usize, ptr: &NonNull<u8>) {
    unsafe {
        dealloc(
            ptr.as_ptr(),
            Layout::from_size_align_unchecked(size, ALIGNMENT),
        )
    }
}
