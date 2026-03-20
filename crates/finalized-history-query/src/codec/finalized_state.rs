use bytes::Bytes;

use crate::domain::types::{BlockRecord, PublicationState};
use crate::error::{Error, Result};

const PUBLICATION_STATE_VERSION: u8 = 3;

pub fn encode_publication_state(state: &PublicationState) -> Bytes {
    let mut out = Vec::with_capacity(49);
    out.push(PUBLICATION_STATE_VERSION);
    out.extend_from_slice(&state.owner_id.to_be_bytes());
    out.extend_from_slice(&state.session_id);
    out.extend_from_slice(&state.epoch.to_be_bytes());
    out.extend_from_slice(&state.indexed_finalized_head.to_be_bytes());
    out.extend_from_slice(&state.lease_valid_through_block.to_be_bytes());
    Bytes::from(out)
}

pub fn decode_publication_state(bytes: &[u8]) -> Result<PublicationState> {
    if bytes.len() != 49 {
        return Err(Error::Decode("invalid publication_state length"));
    }
    if bytes[0] != PUBLICATION_STATE_VERSION {
        return Err(Error::Decode("invalid publication_state version"));
    }

    let mut owner_id = [0u8; 8];
    let mut session_id = [0u8; 16];
    let mut epoch = [0u8; 8];
    let mut indexed_finalized_head = [0u8; 8];
    let mut lease_valid_through_block = [0u8; 8];
    owner_id.copy_from_slice(&bytes[1..9]);
    session_id.copy_from_slice(&bytes[9..25]);
    epoch.copy_from_slice(&bytes[25..33]);
    indexed_finalized_head.copy_from_slice(&bytes[33..41]);
    lease_valid_through_block.copy_from_slice(&bytes[41..49]);
    Ok(PublicationState {
        owner_id: u64::from_be_bytes(owner_id),
        session_id,
        epoch: u64::from_be_bytes(epoch),
        indexed_finalized_head: u64::from_be_bytes(indexed_finalized_head),
        lease_valid_through_block: u64::from_be_bytes(lease_valid_through_block),
    })
}

pub fn encode_block_record(meta: &BlockRecord) -> Bytes {
    let mut out = Vec::with_capacity(76);
    out.extend_from_slice(&meta.block_hash);
    out.extend_from_slice(&meta.parent_hash);
    out.extend_from_slice(&meta.first_log_id.to_be_bytes());
    out.extend_from_slice(&meta.count.to_be_bytes());
    Bytes::from(out)
}

pub fn decode_block_record(bytes: &[u8]) -> Result<BlockRecord> {
    if bytes.len() != 76 {
        return Err(Error::Decode("invalid block_record length"));
    }
    let mut block_hash = [0u8; 32];
    let mut parent_hash = [0u8; 32];
    block_hash.copy_from_slice(&bytes[0..32]);
    parent_hash.copy_from_slice(&bytes[32..64]);
    let mut first_log_id = [0u8; 8];
    first_log_id.copy_from_slice(&bytes[64..72]);
    let mut count = [0u8; 4];
    count.copy_from_slice(&bytes[72..76]);

    Ok(BlockRecord {
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
    fn roundtrip_publication_state_and_block_record() {
        let publication_state = PublicationState {
            owner_id: 8,
            session_id: [7u8; 16],
            epoch: 13,
            indexed_finalized_head: 21,
            lease_valid_through_block: 34,
        };
        assert_eq!(
            decode_publication_state(&encode_publication_state(&publication_state))
                .expect("decode publication state"),
            publication_state
        );

        let meta = BlockRecord {
            block_hash: [1u8; 32],
            parent_hash: [2u8; 32],
            first_log_id: 77,
            count: 99,
        };
        let dec_meta = decode_block_record(&encode_block_record(&meta)).expect("decode meta");
        assert_eq!(dec_meta.first_log_id, meta.first_log_id);
        assert_eq!(dec_meta.count, meta.count);
        assert_eq!(dec_meta.block_hash, meta.block_hash);
        assert_eq!(dec_meta.parent_hash, meta.parent_hash);
    }
}
