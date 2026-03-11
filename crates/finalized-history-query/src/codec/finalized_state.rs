use bytes::Bytes;

use crate::domain::types::{BlockMeta, IndexedHead, MetaState, WriterLease};
use crate::error::{Error, Result};

pub fn encode_meta_state(state: &MetaState) -> Bytes {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&state.indexed_finalized_head.to_be_bytes());
    out.extend_from_slice(&state.next_log_id.to_be_bytes());
    out.extend_from_slice(&state.writer_epoch.to_be_bytes());
    Bytes::from(out)
}

pub fn decode_meta_state(bytes: &[u8]) -> Result<MetaState> {
    if bytes.len() != 24 {
        return Err(Error::Decode("invalid meta/state length"));
    }
    let mut a = [0u8; 8];
    let mut b = [0u8; 8];
    let mut c = [0u8; 8];
    a.copy_from_slice(&bytes[0..8]);
    b.copy_from_slice(&bytes[8..16]);
    c.copy_from_slice(&bytes[16..24]);
    Ok(MetaState {
        indexed_finalized_head: u64::from_be_bytes(a),
        next_log_id: u64::from_be_bytes(b),
        writer_epoch: u64::from_be_bytes(c),
    })
}

pub fn encode_indexed_head(head: &IndexedHead) -> Bytes {
    Bytes::copy_from_slice(&head.indexed_finalized_head.to_be_bytes())
}

pub fn decode_indexed_head(bytes: &[u8]) -> Result<IndexedHead> {
    Ok(IndexedHead {
        indexed_finalized_head: decode_u64(bytes)?,
    })
}

pub fn encode_writer_lease(lease: &WriterLease) -> Bytes {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&lease.owner_id.to_be_bytes());
    out.extend_from_slice(&lease.epoch.to_be_bytes());
    Bytes::from(out)
}

pub fn decode_writer_lease(bytes: &[u8]) -> Result<WriterLease> {
    if bytes.len() != 16 {
        return Err(Error::Decode("invalid writer_lease length"));
    }

    let mut owner_id = [0u8; 8];
    let mut epoch = [0u8; 8];
    owner_id.copy_from_slice(&bytes[0..8]);
    epoch.copy_from_slice(&bytes[8..16]);
    Ok(WriterLease {
        owner_id: u64::from_be_bytes(owner_id),
        epoch: u64::from_be_bytes(epoch),
    })
}

pub fn encode_block_meta(meta: &BlockMeta) -> Bytes {
    let mut out = Vec::with_capacity(76);
    out.extend_from_slice(&meta.block_hash);
    out.extend_from_slice(&meta.parent_hash);
    out.extend_from_slice(&meta.first_log_id.to_be_bytes());
    out.extend_from_slice(&meta.count.to_be_bytes());
    Bytes::from(out)
}

pub fn decode_block_meta(bytes: &[u8]) -> Result<BlockMeta> {
    if bytes.len() != 76 {
        return Err(Error::Decode("invalid block_meta length"));
    }
    let mut block_hash = [0u8; 32];
    let mut parent_hash = [0u8; 32];
    block_hash.copy_from_slice(&bytes[0..32]);
    parent_hash.copy_from_slice(&bytes[32..64]);
    let mut first_log_id = [0u8; 8];
    first_log_id.copy_from_slice(&bytes[64..72]);
    let mut count = [0u8; 4];
    count.copy_from_slice(&bytes[72..76]);

    Ok(BlockMeta {
        block_hash,
        parent_hash,
        first_log_id: u64::from_be_bytes(first_log_id),
        count: u32::from_be_bytes(count),
    })
}

pub fn encode_u64(v: u64) -> Bytes {
    Bytes::copy_from_slice(&v.to_be_bytes())
}

pub fn decode_u64(bytes: &[u8]) -> Result<u64> {
    if bytes.len() != 8 {
        return Err(Error::Decode("invalid u64 length"));
    }
    let mut v = [0u8; 8];
    v.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_state_and_block_meta() {
        let state = MetaState {
            indexed_finalized_head: 10,
            next_log_id: 55,
            writer_epoch: 9,
        };
        let dec_state = decode_meta_state(&encode_meta_state(&state)).expect("decode state");
        assert_eq!(dec_state, state);

        let meta = BlockMeta {
            block_hash: [1u8; 32],
            parent_hash: [2u8; 32],
            first_log_id: 77,
            count: 99,
        };
        let dec_meta = decode_block_meta(&encode_block_meta(&meta)).expect("decode meta");
        assert_eq!(dec_meta.first_log_id, meta.first_log_id);
        assert_eq!(dec_meta.count, meta.count);
        assert_eq!(dec_meta.block_hash, meta.block_hash);
        assert_eq!(dec_meta.parent_hash, meta.parent_hash);

        let head = IndexedHead {
            indexed_finalized_head: 17,
        };
        assert_eq!(
            decode_indexed_head(&encode_indexed_head(&head))
                .expect("decode indexed head")
                .indexed_finalized_head,
            17
        );

        let lease = WriterLease {
            owner_id: 3,
            epoch: 12,
        };
        assert_eq!(
            decode_writer_lease(&encode_writer_lease(&lease)).expect("decode writer lease"),
            lease
        );
    }
}
