use bytes::Bytes;

use crate::domain::types::PublicationState;
use crate::error::{Error, Result};

const PUBLICATION_STATE_VERSION: u8 = 4;

fixed_codec! {
    impl PublicationState {
        length_error = "invalid publication_state length";
        version = PUBLICATION_STATE_VERSION;
        version_error = "invalid publication_state version";
        fields {
            owner_id: u64,
            session_id: [u8; 16],
            indexed_finalized_head: u64,
            lease_valid_through_block: u64,
        }
    }
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
    fn roundtrip_publication_state() {
        let publication_state = PublicationState {
            owner_id: 8,
            session_id: [7u8; 16],
            indexed_finalized_head: 21,
            lease_valid_through_block: 34,
        };
        assert_eq!(
            PublicationState::decode(&publication_state.encode())
                .expect("decode publication state"),
            publication_state
        );
    }
}
