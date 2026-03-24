use bytes::Bytes;

use crate::error::Result;
use crate::kernel::codec::StorageCodec;
use crate::txs::types::BlockTxHeader;

pub fn validate_tx(_tx_bytes: &[u8]) -> bool {
    todo!("tx payload validation is not implemented")
}

impl StorageCodec for BlockTxHeader {
    fn encode(&self) -> Bytes {
        todo!("tx block-header encoding is not implemented")
    }

    fn decode(_bytes: &[u8]) -> Result<Self> {
        todo!("tx block-header decoding is not implemented")
    }
}
