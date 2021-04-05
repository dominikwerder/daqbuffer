use libc::{size_t};

extern {
    pub fn bshuf_compress_lz4(inp: *const u8, out: *const u8, size: size_t, elem_size: size_t, block_size: size_t) -> i64;
    pub fn bshuf_decompress_lz4(inp: *const u8, out: *const u8, size: size_t, elem_size: size_t, block_size: size_t) -> i64;
}

pub fn bitshuffle_compress(inp: &[u8], out: &mut [u8], size: usize, elem_size: usize, block_size: usize) -> Result<usize, isize> {
    unsafe {
        let n = bshuf_compress_lz4(inp.as_ptr(), out.as_mut_ptr(), size, elem_size, block_size);
        if n >= 0 {
            Ok(n as usize)
        }
        else {
            Err(n as isize)
        }
    }
}

pub fn bitshuffle_decompress(inp: &[u8], out: &mut [u8], size: usize, elem_size: usize, block_size: usize) -> Result<usize, isize> {
    unsafe {
        let n = bshuf_decompress_lz4(inp.as_ptr(), out.as_mut_ptr(), size, elem_size, block_size);
        if n >= 0 {
            Ok(n as usize)
        }
        else {
            Err(n as isize)
        }
    }
}
