use alloy_rlp::{Header, PayloadView};
use bytes::Bytes;

use crate::error::{Error, Result};
use crate::family::Hash32;
use crate::txs::types::{Address20, Selector4};

#[derive(Clone, PartialEq, Eq)]
pub struct Tx {
    block_num: u64,
    block_hash: Hash32,
    tx_idx: u32,
    envelope_bytes: Bytes,
}

impl Tx {
    pub fn new(
        block_num: u64,
        block_hash: Hash32,
        tx_idx: u32,
        envelope_bytes: Bytes,
    ) -> Result<Self> {
        TxEnvelopeView::decode(envelope_bytes.as_ref())?;
        Ok(Self {
            block_num,
            block_hash,
            tx_idx,
            envelope_bytes,
        })
    }

    pub fn block_num(&self) -> u64 {
        self.block_num
    }

    pub fn block_hash(&self) -> &Hash32 {
        &self.block_hash
    }

    pub fn tx_idx(&self) -> u32 {
        self.tx_idx
    }

    pub fn envelope(&self) -> Result<TxEnvelopeView<'_>> {
        TxEnvelopeView::decode(self.envelope_bytes.as_ref())
    }

    pub fn tx_hash(&self) -> Result<&Hash32> {
        Ok(self.envelope()?.tx_hash())
    }

    pub fn sender(&self) -> Result<&Address20> {
        Ok(self.envelope()?.sender())
    }

    pub fn signed_tx_bytes(&self) -> Result<&[u8]> {
        Ok(self.envelope()?.signed_tx_bytes())
    }

    pub fn signed_tx(&self) -> Result<TxView<'_>> {
        self.envelope()?.signed_tx()
    }
}

impl std::fmt::Debug for Tx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tx")
            .field("block_num", &self.block_num)
            .field("tx_idx", &self.tx_idx)
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TxEnvelopeView<'a> {
    tx_hash: &'a Hash32,
    sender: &'a Address20,
    signed_tx_bytes: &'a [u8],
}

impl<'a> TxEnvelopeView<'a> {
    pub fn decode(envelope_bytes: &'a [u8]) -> Result<Self> {
        let mut buf = envelope_bytes;
        let PayloadView::List(fields) =
            Header::decode_raw(&mut buf).map_err(|_| Error::Decode("invalid tx envelope rlp"))?
        else {
            return Err(Error::Decode("tx envelope must be an rlp list"));
        };
        if !buf.is_empty() {
            return Err(Error::Decode("tx envelope has trailing bytes"));
        }
        if fields.len() != 3 {
            return Err(Error::Decode("unexpected tx envelope field count"));
        }

        Ok(Self {
            tx_hash: decode_hash(fields[0])?,
            sender: decode_sender(fields[1])?,
            signed_tx_bytes: decode_payload_bytes(fields[2])?,
        })
    }

    pub fn tx_hash(&self) -> &'a Hash32 {
        self.tx_hash
    }

    pub fn sender(&self) -> &'a Address20 {
        self.sender
    }

    pub fn signed_tx_bytes(&self) -> &'a [u8] {
        self.signed_tx_bytes
    }

    pub fn signed_tx(&self) -> Result<TxView<'a>> {
        TxView::decode(self.signed_tx_bytes)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TxView<'a> {
    Legacy(LegacyTxView<'a>),
    Eip2930(Eip2930TxView<'a>),
    Eip1559(Eip1559TxView<'a>),
    Eip4844(Eip4844TxView<'a>),
}

fn decode_hash(field: &[u8]) -> Result<&Hash32> {
    let payload = decode_payload_bytes(field)?;
    payload
        .try_into()
        .map_err(|_| Error::Decode("invalid tx envelope hash"))
}

fn decode_sender(field: &[u8]) -> Result<&Address20> {
    let payload = decode_payload_bytes(field)?;
    payload
        .try_into()
        .map_err(|_| Error::Decode("invalid tx envelope sender"))
}

fn decode_payload_bytes(field: &[u8]) -> Result<&[u8]> {
    decode_rlp_string_payload(field)
}

fn decode_rlp_string_payload(field: &[u8]) -> Result<&[u8]> {
    let mut buf = field;
    match Header::decode_raw(&mut buf) {
        Ok(PayloadView::String(payload)) if buf.is_empty() => Ok(payload),
        Ok(PayloadView::List(_)) if buf.is_empty() => {
            Err(Error::Decode("tx envelope field must be bytes"))
        }
        Ok(_) if buf.is_empty() => Err(Error::Decode("tx envelope field must be bytes")),
        Ok(_) => Err(Error::Decode("tx envelope field has trailing bytes")),
        Err(_) => Err(Error::Decode("invalid tx envelope field")),
    }
}

#[cfg(test)]
mod tests {
    use crate::kernel::codec::StorageCodec;
    use crate::txs::types::StoredTxEnvelope;

    use super::*;

    #[test]
    fn tx_envelope_view_decodes_outer_fields() {
        let envelope = StoredTxEnvelope {
            tx_hash: [1u8; 32],
            sender: [2u8; 20],
            signed_tx_bytes: vec![3, 4, 5],
        };
        let encoded = envelope.encode();
        let view = TxEnvelopeView::decode(&encoded).expect("decode envelope");

        assert_eq!(view.tx_hash(), &[1u8; 32]);
        assert_eq!(view.sender(), &[2u8; 20]);
        assert_eq!(view.signed_tx_bytes(), &[3, 4, 5]);
    }

    #[test]
    fn tx_wraps_envelope_bytes_zero_copy() {
        let envelope = StoredTxEnvelope {
            tx_hash: [1u8; 32],
            sender: [2u8; 20],
            signed_tx_bytes: vec![3, 4, 5],
        };
        let tx = Tx::new(7, [9u8; 32], 4, envelope.encode()).expect("tx");

        assert_eq!(tx.block_num(), 7);
        assert_eq!(tx.block_hash(), &[9u8; 32]);
        assert_eq!(tx.tx_idx(), 4);
        assert_eq!(tx.tx_hash().expect("tx_hash"), &[1u8; 32]);
        assert_eq!(tx.sender().expect("sender"), &[2u8; 20]);
        assert_eq!(tx.signed_tx_bytes().expect("signed bytes"), &[3, 4, 5]);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LegacyTxView<'a>(pub &'a [u8]);

#[derive(Debug, Clone, Copy)]
pub struct Eip2930TxView<'a>(pub &'a [u8]);

#[derive(Debug, Clone, Copy)]
pub struct Eip1559TxView<'a>(pub &'a [u8]);

#[derive(Debug, Clone, Copy)]
pub struct Eip4844TxView<'a>(pub &'a [u8]);

impl<'a> TxView<'a> {
    pub fn decode(_tx_bytes: &'a [u8]) -> Result<Self> {
        todo!("tx zero-copy variant dispatch is not implemented")
    }

    pub fn tx_hash(&self) -> Result<Hash32> {
        todo!("tx hash extraction is not implemented")
    }

    pub fn from_addr(&self) -> Result<Address20> {
        todo!("tx sender extraction is not implemented")
    }

    pub fn to_addr(&self) -> Result<Option<Address20>> {
        todo!("tx recipient extraction is not implemented")
    }

    pub fn selector(&self) -> Result<Option<Selector4>> {
        todo!("tx selector extraction is not implemented")
    }

    pub fn input(&self) -> Result<&'a [u8]> {
        todo!("tx input extraction is not implemented")
    }
}
