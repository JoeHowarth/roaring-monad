use alloy_rlp::{Encodable, Header, PayloadView};
use bytes::Bytes;

use crate::error::{Error, Result};
use crate::kernel::codec::StorageCodec;
use crate::kernel::codec::fixed_codec;
use crate::txs::types::{BlockTxHeader, StoredTxEnvelope, TxLocation};

pub fn validate_tx(_tx_bytes: &[u8]) -> bool {
    todo!("tx payload validation is not implemented")
}

impl StorageCodec for BlockTxHeader {
    fn encode(&self) -> Bytes {
        let offsets = self.offsets.encode();
        let mut out = Vec::with_capacity(1 + offsets.len());
        out.push(1);
        out.extend_from_slice(&offsets);
        Bytes::from(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 1 + 1 + 4 {
            return Err(Error::Decode("block tx header too short"));
        }
        if bytes[0] != 1 {
            return Err(Error::Decode("unsupported block tx header version"));
        }
        let offsets = crate::core::offsets::BucketedOffsets::decode(&bytes[1..])?;
        if offsets.is_empty() {
            return Err(Error::Decode("block tx header missing sentinel"));
        }
        Ok(Self { offsets })
    }
}

fixed_codec! {
    impl TxLocation {
        length_error = "invalid tx location length";
        version = 1;
        version_error = "invalid tx location version";
        fields {
            block_num: u64,
            tx_idx: u32,
        }
    }
}

impl StorageCodec for StoredTxEnvelope {
    fn encode(&self) -> Bytes {
        let tx_hash = self.tx_hash.as_slice();
        let sender = self.sender.as_slice();
        let signed_tx_bytes = self.signed_tx_bytes.as_slice();
        let payload_length = tx_hash.length() + sender.length() + signed_tx_bytes.length();
        let mut out = Vec::new();
        Header {
            list: true,
            payload_length,
        }
        .encode(&mut out);
        tx_hash.encode(&mut out);
        sender.encode(&mut out);
        signed_tx_bytes.encode(&mut out);
        Bytes::from(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut buf = bytes;
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

        let tx_hash = decode_fixed::<32>(fields[0], "invalid tx envelope hash")?;
        let sender = decode_fixed::<20>(fields[1], "invalid tx envelope sender")?;
        let signed_tx_bytes = decode_bytes(fields[2], "invalid tx envelope tx bytes")?;
        Ok(Self {
            tx_hash,
            sender,
            signed_tx_bytes: signed_tx_bytes.to_vec(),
        })
    }
}

fn decode_fixed<const N: usize>(field: &[u8], message: &'static str) -> Result<[u8; N]> {
    let value = decode_bytes(field, message)?;
    if value.len() != N {
        return Err(Error::Decode(message));
    }
    let mut out = [0u8; N];
    out.copy_from_slice(value);
    Ok(out)
}

fn decode_bytes<'a>(field: &'a [u8], _message: &'static str) -> Result<&'a [u8]> {
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
    use crate::core::offsets::BucketedOffsets;

    use super::*;

    #[test]
    fn roundtrip_block_tx_header() {
        let mut offsets = BucketedOffsets::new();
        offsets.push(0).expect("offset");
        offsets.push(7).expect("offset");
        offsets.push(4_294_967_301).expect("offset");
        let header = BlockTxHeader { offsets };

        let encoded = header.encode();
        let decoded = BlockTxHeader::decode(&encoded).expect("decode header");
        assert_eq!(decoded, header);
    }

    #[test]
    fn roundtrip_tx_location() {
        let location = TxLocation {
            block_num: 77,
            tx_idx: 5,
        };
        let encoded = location.encode();
        let decoded = TxLocation::decode(&encoded).expect("decode location");
        assert_eq!(decoded, location);
    }

    #[test]
    fn roundtrip_stored_tx_envelope() {
        let envelope = StoredTxEnvelope {
            tx_hash: [7u8; 32],
            sender: [9u8; 20],
            signed_tx_bytes: vec![1, 2, 3, 4, 5],
        };
        let encoded = envelope.encode();
        let decoded = StoredTxEnvelope::decode(&encoded).expect("decode envelope");
        assert_eq!(decoded, envelope);
    }
}
