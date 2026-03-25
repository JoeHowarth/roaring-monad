use alloy_rlp::{Header, PayloadView};
use bytes::Bytes;

use crate::core::offsets::byte_offset_in;
use crate::error::{Error, Result};
use crate::family::Hash32;
use crate::txs::types::{Address20, Selector4};

const LEGACY_TX_FIELD_COUNT: usize = 9;
const EIP2930_TX_FIELD_COUNT: usize = 11;
const EIP1559_TX_FIELD_COUNT: usize = 12;
const EIP4844_TX_FIELD_COUNT: usize = 14;
const EIP2930_TYPE: u8 = 0x01;
const EIP1559_TYPE: u8 = 0x02;
const EIP4844_TYPE: u8 = 0x03;

#[derive(Clone, PartialEq, Eq)]
pub struct TxRef {
    block_num: u64,
    block_hash: Hash32,
    tx_idx: u32,
    envelope_bytes: Bytes,
    offsets: TxRefOffsets,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TxRefOffsets {
    signed_tx_field_start: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SignedTxOffsets {
    to_field_start: u32,
    input_field_start: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxKind {
    Legacy,
    Eip2930,
    Eip1559,
    Eip4844,
}

impl TxRef {
    pub fn new(
        block_num: u64,
        block_hash: Hash32,
        tx_idx: u32,
        envelope_bytes: Bytes,
    ) -> Result<Self> {
        let offsets = parse_tx_ref_offsets(envelope_bytes.as_ref())?;
        Ok(Self {
            block_num,
            block_hash,
            tx_idx,
            envelope_bytes,
            offsets,
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
        let tx_hash_field_start = envelope_payload_start(self.envelope_bytes.as_ref())?;
        decode_hash(field_at(self.envelope_bytes.as_ref(), tx_hash_field_start)?)
    }

    pub fn sender(&self) -> Result<&Address20> {
        let tx_hash_field_start = envelope_payload_start(self.envelope_bytes.as_ref())?;
        let sender_field_start =
            next_field_start(self.envelope_bytes.as_ref(), tx_hash_field_start)?;
        decode_sender(field_at(self.envelope_bytes.as_ref(), sender_field_start)?)
    }

    pub fn signed_tx_bytes(&self) -> Result<&[u8]> {
        decode_payload_bytes(field_at(
            self.envelope_bytes.as_ref(),
            self.offsets.signed_tx_field_start as usize,
        )?)
    }

    pub fn signed_tx(&self) -> Result<TxView<'_>> {
        TxView::decode(self.signed_tx_bytes()?)
    }

    pub fn to_addr(&self) -> Result<Option<Address20>> {
        let signed_tx_bytes = self.signed_tx_bytes()?;
        let offsets = parse_signed_tx_offsets(signed_tx_bytes)?;
        decode_optional_address(field_at(signed_tx_bytes, offsets.to_field_start as usize)?)
    }

    pub fn selector(&self) -> Result<Option<Selector4>> {
        if self.to_addr()?.is_none() {
            return Ok(None);
        }
        let input = self.input()?;
        Ok((input.len() >= 4).then(|| input[..4].try_into().expect("4-byte selector")))
    }

    pub fn input(&self) -> Result<&[u8]> {
        let signed_tx_bytes = self.signed_tx_bytes()?;
        let offsets = parse_signed_tx_offsets(signed_tx_bytes)?;
        decode_payload_bytes(field_at(
            signed_tx_bytes,
            offsets.input_field_start as usize,
        )?)
    }
}

impl std::fmt::Debug for TxRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxRef")
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

fn parse_tx_ref_offsets(envelope_bytes: &[u8]) -> Result<TxRefOffsets> {
    let payload_start = envelope_payload_start(envelope_bytes)?;
    let sender_field_start = next_field_start(envelope_bytes, payload_start)?;
    let signed_tx_field_start = next_field_start(envelope_bytes, sender_field_start)?;
    Ok(TxRefOffsets {
        signed_tx_field_start: signed_tx_field_start
            .try_into()
            .map_err(|_| Error::Decode("signed tx field start overflow"))?,
    })
}

fn parse_signed_tx_offsets(tx_bytes: &[u8]) -> Result<SignedTxOffsets> {
    let (kind, payload, base_offset) = match tx_bytes.split_first() {
        Some((&EIP2930_TYPE, payload)) => (TxKind::Eip2930, payload, 1usize),
        Some((&EIP1559_TYPE, payload)) => (TxKind::Eip1559, payload, 1usize),
        Some((&EIP4844_TYPE, payload)) => (TxKind::Eip4844, payload, 1usize),
        Some(_) => (TxKind::Legacy, tx_bytes, 0usize),
        None => return Err(Error::Decode("signed tx bytes are empty")),
    };
    let payload_start = first_list_field_start(
        payload,
        "invalid signed tx rlp",
        "signed tx payload must be an rlp list",
    )?;
    let (to_index, input_index) = match kind {
        TxKind::Legacy => (3usize, 5usize),
        TxKind::Eip2930 => (4usize, 6usize),
        TxKind::Eip1559 => (5usize, 7usize),
        TxKind::Eip4844 => (5usize, 7usize),
    };

    let mut current = payload_start;
    let mut to_field_start = None;
    let mut input_field_start = None;
    for index in 0..=input_index {
        if index == to_index {
            to_field_start = Some(base_offset + current);
        }
        if index == input_index {
            input_field_start = Some(base_offset + current);
            break;
        }
        current = next_field_start(payload, current)?;
    }

    Ok(SignedTxOffsets {
        to_field_start: u32::try_from(to_field_start.ok_or(Error::Decode("missing tx to field"))?)
            .map_err(|_| Error::Decode("tx to field start overflow"))?,
        input_field_start: u32::try_from(
            input_field_start.ok_or(Error::Decode("missing tx input field"))?,
        )
        .map_err(|_| Error::Decode("tx input field start overflow"))?,
    })
}

#[derive(Debug, Clone, Copy)]
pub enum TxView<'a> {
    Legacy(LegacyTxView<'a>),
    Eip2930(Eip2930TxView<'a>),
    Eip1559(Eip1559TxView<'a>),
    Eip4844(Eip4844TxView<'a>),
}

#[derive(Debug, Clone, Copy)]
pub struct LegacyTxView<'a> {
    _tx_bytes: &'a [u8],
    fields: [&'a [u8]; LEGACY_TX_FIELD_COUNT],
}

#[derive(Debug, Clone, Copy)]
pub struct Eip2930TxView<'a> {
    _tx_bytes: &'a [u8],
    fields: [&'a [u8]; EIP2930_TX_FIELD_COUNT],
}

#[derive(Debug, Clone, Copy)]
pub struct Eip1559TxView<'a> {
    _tx_bytes: &'a [u8],
    fields: [&'a [u8]; EIP1559_TX_FIELD_COUNT],
}

#[derive(Debug, Clone, Copy)]
pub struct Eip4844TxView<'a> {
    _tx_bytes: &'a [u8],
    fields: [&'a [u8]; EIP4844_TX_FIELD_COUNT],
}

impl<'a> TxView<'a> {
    pub fn decode(tx_bytes: &'a [u8]) -> Result<Self> {
        let Some((&tx_type, payload)) = tx_bytes.split_first() else {
            return Err(Error::Decode("signed tx bytes are empty"));
        };

        match tx_type {
            EIP2930_TYPE => Ok(Self::Eip2930(Eip2930TxView::new(tx_bytes, payload)?)),
            EIP1559_TYPE => Ok(Self::Eip1559(Eip1559TxView::new(tx_bytes, payload)?)),
            EIP4844_TYPE => Ok(Self::Eip4844(Eip4844TxView::new(tx_bytes, payload)?)),
            _ => Ok(Self::Legacy(LegacyTxView::new(tx_bytes)?)),
        }
    }

    pub fn tx_hash(&self) -> Result<Hash32> {
        Err(Error::Decode("tx hash is carried by the outer tx envelope"))
    }

    pub fn from_addr(&self) -> Result<Address20> {
        Err(Error::Decode("sender is carried by the outer tx envelope"))
    }

    pub fn to_addr(&self) -> Result<Option<Address20>> {
        match self {
            Self::Legacy(view) => view.recipient(),
            Self::Eip2930(view) => view.recipient(),
            Self::Eip1559(view) => view.recipient(),
            Self::Eip4844(view) => view.recipient(),
        }
    }

    pub fn selector(&self) -> Result<Option<Selector4>> {
        if self.to_addr()?.is_none() {
            return Ok(None);
        }
        let input = self.input()?;
        Ok((input.len() >= 4).then(|| input[..4].try_into().expect("4-byte selector")))
    }

    pub fn input(&self) -> Result<&'a [u8]> {
        match self {
            Self::Legacy(view) => view.input(),
            Self::Eip2930(view) => view.input(),
            Self::Eip1559(view) => view.input(),
            Self::Eip4844(view) => view.input(),
        }
    }
}

impl<'a> LegacyTxView<'a> {
    fn new(tx_bytes: &'a [u8]) -> Result<Self> {
        Ok(Self {
            _tx_bytes: tx_bytes,
            fields: parse_tx_fields::<LEGACY_TX_FIELD_COUNT>(
                tx_bytes,
                "invalid legacy tx rlp",
                "legacy tx must be an rlp list",
            )?,
        })
    }

    fn recipient(&self) -> Result<Option<Address20>> {
        decode_optional_address(self.fields[3])
    }

    fn input(&self) -> Result<&'a [u8]> {
        decode_payload_bytes(self.fields[5])
    }
}

impl<'a> Eip2930TxView<'a> {
    fn new(tx_bytes: &'a [u8], payload: &'a [u8]) -> Result<Self> {
        Ok(Self {
            _tx_bytes: tx_bytes,
            fields: parse_tx_fields::<EIP2930_TX_FIELD_COUNT>(
                payload,
                "invalid eip-2930 tx rlp",
                "eip-2930 tx payload must be an rlp list",
            )?,
        })
    }

    fn recipient(&self) -> Result<Option<Address20>> {
        decode_optional_address(self.fields[4])
    }

    fn input(&self) -> Result<&'a [u8]> {
        decode_payload_bytes(self.fields[6])
    }
}

impl<'a> Eip1559TxView<'a> {
    fn new(tx_bytes: &'a [u8], payload: &'a [u8]) -> Result<Self> {
        Ok(Self {
            _tx_bytes: tx_bytes,
            fields: parse_tx_fields::<EIP1559_TX_FIELD_COUNT>(
                payload,
                "invalid eip-1559 tx rlp",
                "eip-1559 tx payload must be an rlp list",
            )?,
        })
    }

    fn recipient(&self) -> Result<Option<Address20>> {
        decode_optional_address(self.fields[5])
    }

    fn input(&self) -> Result<&'a [u8]> {
        decode_payload_bytes(self.fields[7])
    }
}

impl<'a> Eip4844TxView<'a> {
    fn new(tx_bytes: &'a [u8], payload: &'a [u8]) -> Result<Self> {
        Ok(Self {
            _tx_bytes: tx_bytes,
            fields: parse_tx_fields::<EIP4844_TX_FIELD_COUNT>(
                payload,
                "invalid eip-4844 tx rlp",
                "eip-4844 tx payload must be an rlp list",
            )?,
        })
    }

    fn recipient(&self) -> Result<Option<Address20>> {
        decode_optional_address(self.fields[5])
    }

    fn input(&self) -> Result<&'a [u8]> {
        decode_payload_bytes(self.fields[7])
    }
}

fn parse_tx_fields<'a, const N: usize>(
    tx_payload: &'a [u8],
    invalid_rlp_message: &'static str,
    invalid_kind_message: &'static str,
) -> Result<[&'a [u8]; N]> {
    let mut buf = tx_payload;
    let PayloadView::List(fields) =
        Header::decode_raw(&mut buf).map_err(|_| Error::Decode(invalid_rlp_message))?
    else {
        return Err(Error::Decode(invalid_kind_message));
    };
    if !buf.is_empty() {
        return Err(Error::Decode("signed tx has trailing bytes"));
    }
    fields
        .try_into()
        .map_err(|_| Error::Decode("unexpected signed tx field count"))
}

fn envelope_payload_start(envelope_bytes: &[u8]) -> Result<usize> {
    first_list_field_start(
        envelope_bytes,
        "invalid tx envelope rlp",
        "tx envelope must be an rlp list",
    )
}

fn first_list_field_start(
    bytes: &[u8],
    invalid_rlp_message: &'static str,
    invalid_kind_message: &'static str,
) -> Result<usize> {
    let mut buf = bytes;
    let PayloadView::List(fields) =
        Header::decode_raw(&mut buf).map_err(|_| Error::Decode(invalid_rlp_message))?
    else {
        return Err(Error::Decode(invalid_kind_message));
    };
    if !buf.is_empty() {
        return Err(Error::Decode("signed tx has trailing bytes"));
    }
    let first_field = fields
        .first()
        .ok_or(Error::Decode("unexpected signed tx field count"))?;
    usize::try_from(byte_offset_in(bytes, first_field))
        .map_err(|_| Error::Decode("tx field offset overflow"))
}

fn field_at(bytes: &[u8], start: usize) -> Result<&[u8]> {
    let mut buf = bytes
        .get(start..)
        .ok_or(Error::Decode("tx field start out of bounds"))?;
    Header::decode_raw(&mut buf).map_err(|_| Error::Decode("invalid tx field"))?;
    let end = bytes.len().saturating_sub(buf.len());
    bytes
        .get(start..end)
        .ok_or(Error::Decode("invalid tx field"))
}

fn next_field_start(bytes: &[u8], start: usize) -> Result<usize> {
    let mut buf = bytes
        .get(start..)
        .ok_or(Error::Decode("tx field start out of bounds"))?;
    Header::decode_raw(&mut buf).map_err(|_| Error::Decode("invalid tx field"))?;
    Ok(bytes.len().saturating_sub(buf.len()))
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

fn decode_optional_address(field: &[u8]) -> Result<Option<Address20>> {
    let payload = decode_payload_bytes(field)?;
    if payload.is_empty() {
        return Ok(None);
    }
    payload
        .try_into()
        .map(Some)
        .map_err(|_| Error::Decode("invalid tx recipient"))
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
    use alloy_rlp::Encodable;

    use crate::kernel::codec::StorageCodec;
    use crate::txs::types::StoredTxEnvelope;

    use super::*;

    fn encode_field<T: Encodable>(value: T) -> Vec<u8> {
        let mut out = Vec::new();
        value.encode(&mut out);
        out
    }

    fn encode_bytes(value: &[u8]) -> Vec<u8> {
        encode_field(value)
    }

    fn encode_tx_list(fields: Vec<Vec<u8>>) -> Vec<u8> {
        let mut out = Vec::new();
        Header {
            list: true,
            payload_length: fields.iter().map(Vec::len).sum(),
        }
        .encode(&mut out);
        for field in fields {
            out.extend_from_slice(&field);
        }
        out
    }

    fn encode_legacy_tx(to: Option<[u8; 20]>, input: &[u8]) -> Vec<u8> {
        encode_tx_list(vec![
            encode_field(1u64),
            encode_field(2u64),
            encode_field(21_000u64),
            encode_bytes(to.as_ref().map(<[u8; 20]>::as_slice).unwrap_or(&[])),
            encode_field(3u64),
            encode_bytes(input),
            encode_field(27u8),
            encode_field(1u8),
            encode_field(2u8),
        ])
    }

    fn encode_eip1559_tx(to: Option<[u8; 20]>, input: &[u8]) -> Vec<u8> {
        let payload = encode_tx_list(vec![
            encode_field(1u64),
            encode_field(2u64),
            encode_field(3u64),
            encode_field(4u64),
            encode_field(21_000u64),
            encode_bytes(to.as_ref().map(<[u8; 20]>::as_slice).unwrap_or(&[])),
            encode_field(5u64),
            encode_bytes(input),
            encode_tx_list(Vec::new()),
            encode_field(1u8),
            encode_field(2u8),
            encode_field(3u8),
        ]);
        let mut tx = vec![EIP1559_TYPE];
        tx.extend_from_slice(&payload);
        tx
    }

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
        let tx = TxRef::new(7, [9u8; 32], 4, envelope.encode()).expect("tx");

        assert_eq!(tx.block_num(), 7);
        assert_eq!(tx.block_hash(), &[9u8; 32]);
        assert_eq!(tx.tx_idx(), 4);
        assert_eq!(tx.tx_hash().expect("tx_hash"), &[1u8; 32]);
        assert_eq!(tx.sender().expect("sender"), &[2u8; 20]);
        assert_eq!(tx.signed_tx_bytes().expect("signed bytes"), &[3, 4, 5]);
    }

    #[test]
    fn legacy_tx_view_decodes_to_and_selector() {
        let encoded = encode_legacy_tx(Some([9u8; 20]), &[0xaa, 0xbb, 0xcc, 0xdd, 1]);
        let tx = TxView::decode(&encoded).expect("decode");

        assert_eq!(tx.to_addr().expect("to"), Some([9u8; 20]));
        assert_eq!(
            tx.selector().expect("selector"),
            Some([0xaa, 0xbb, 0xcc, 0xdd])
        );
        assert_eq!(tx.input().expect("input"), &[0xaa, 0xbb, 0xcc, 0xdd, 1]);
    }

    #[test]
    fn typed_tx_view_decodes_create_without_selector() {
        let encoded = encode_eip1559_tx(None, &[0xaa, 0xbb, 0xcc, 0xdd]);
        let tx = TxView::decode(&encoded).expect("decode");

        assert_eq!(tx.to_addr().expect("to"), None);
        assert_eq!(tx.selector().expect("selector"), None);
        assert_eq!(tx.input().expect("input"), &[0xaa, 0xbb, 0xcc, 0xdd]);
    }
}
