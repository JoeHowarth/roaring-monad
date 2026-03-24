pub const LOCAL_ID_BITS: u32 = 24;
pub const LOCAL_ID_MASK: u64 = (1u64 << LOCAL_ID_BITS) - 1;
pub const MAX_LOCAL_ID: u32 = LOCAL_ID_MASK as u32;

pub const DIRECTORY_BUCKET_SIZE: u64 = 1_000_000;
pub const DIRECTORY_SUB_BUCKET_SIZE: u64 = 10_000;

pub fn read_u64_be(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    let mut out = [0u8; 8];
    out.copy_from_slice(bytes);
    Some(u64::from_be_bytes(out))
}
