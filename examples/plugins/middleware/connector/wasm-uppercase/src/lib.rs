use std::{mem, slice};

#[no_mangle]
pub extern "C" fn alloc(len: u32) -> u32 {
    if len == 0 {
        return 0;
    }
    let mut buffer = Vec::<u8>::with_capacity(len as usize);
    let pointer = buffer.as_mut_ptr() as u32;
    mem::forget(buffer);
    pointer
}

#[no_mangle]
pub unsafe extern "C" fn dealloc(pointer: u32, len: u32) {
    if len == 0 {
        return;
    }
    drop(Vec::from_raw_parts(pointer as *mut u8, 0, len as usize));
}

#[no_mangle]
pub unsafe extern "C" fn transform(pointer: u32, len: u32) -> u64 {
    if len == 0 {
        return 0;
    }
    let message = slice::from_raw_parts_mut(pointer as *mut u8, len as usize);
    message.make_ascii_uppercase();
    ((pointer as u64) << 32) | len as u64
}
