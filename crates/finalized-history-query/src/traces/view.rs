use alloy_rlp::{Decodable, Header, PayloadView};
use bytes::Bytes;

use crate::core::offsets::byte_offset_in;
use crate::error::{Error, Result};
use crate::family::Hash32;
use crate::traces::types::{Address20, Selector4};

const CALL_FRAME_FIELD_COUNT: usize = 11;

#[derive(Clone, PartialEq, Eq)]
pub struct TraceRef {
    block_num: u64,
    block_hash: Hash32,
    tx_idx: u32,
    trace_idx: u32,
    frame_bytes: Bytes,
    layout: TraceLayout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TraceLayout {
    typ_field_start: u32,
    flags_field_start: u32,
    from_field_start: u32,
    to_field_start: u32,
    value_field_start: u32,
    gas_field_start: u32,
    gas_used_field_start: u32,
    input_field_start: u32,
    output_field_start: u32,
    status_field_start: u32,
    depth_field_start: u32,
}

impl TraceRef {
    pub fn new(
        block_num: u64,
        block_hash: Hash32,
        tx_idx: u32,
        trace_idx: u32,
        frame_bytes: Bytes,
    ) -> Result<Self> {
        let layout = parse_trace_layout(frame_bytes.as_ref())?;
        Ok(Self {
            block_num,
            block_hash,
            tx_idx,
            trace_idx,
            frame_bytes,
            layout,
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

    pub fn trace_idx(&self) -> u32 {
        self.trace_idx
    }

    pub fn call_frame(&self) -> Result<CallFrameView<'_>> {
        CallFrameView::new(self.frame_bytes.as_ref())
    }

    pub fn typ(&self) -> Result<u8> {
        decode_u8(field_at(
            self.frame_bytes.as_ref(),
            self.layout.typ_field_start as usize,
        )?)
    }

    pub fn flags(&self) -> Result<u64> {
        decode_u64(field_at(
            self.frame_bytes.as_ref(),
            self.layout.flags_field_start as usize,
        )?)
    }

    pub fn from_addr(&self) -> Result<&Address20> {
        decode_address(field_at(
            self.frame_bytes.as_ref(),
            self.layout.from_field_start as usize,
        )?)
    }

    pub fn to_addr(&self) -> Result<Option<&Address20>> {
        decode_optional_address(field_at(
            self.frame_bytes.as_ref(),
            self.layout.to_field_start as usize,
        )?)
    }

    pub fn value_bytes(&self) -> Result<&[u8]> {
        decode_payload_bytes(field_at(
            self.frame_bytes.as_ref(),
            self.layout.value_field_start as usize,
        )?)
    }

    pub fn gas(&self) -> Result<u64> {
        decode_u64(field_at(
            self.frame_bytes.as_ref(),
            self.layout.gas_field_start as usize,
        )?)
    }

    pub fn gas_used(&self) -> Result<u64> {
        decode_u64(field_at(
            self.frame_bytes.as_ref(),
            self.layout.gas_used_field_start as usize,
        )?)
    }

    pub fn input(&self) -> Result<&[u8]> {
        decode_payload_bytes(field_at(
            self.frame_bytes.as_ref(),
            self.layout.input_field_start as usize,
        )?)
    }

    pub fn output(&self) -> Result<&[u8]> {
        decode_payload_bytes(field_at(
            self.frame_bytes.as_ref(),
            self.layout.output_field_start as usize,
        )?)
    }

    pub fn status(&self) -> Result<u8> {
        decode_u8(field_at(
            self.frame_bytes.as_ref(),
            self.layout.status_field_start as usize,
        )?)
    }

    pub fn depth(&self) -> Result<u64> {
        decode_u64(field_at(
            self.frame_bytes.as_ref(),
            self.layout.depth_field_start as usize,
        )?)
    }

    pub fn selector(&self) -> Result<Option<&Selector4>> {
        if !self.is_call_type()? {
            return Ok(None);
        }
        let input = self.input()?;
        Ok((input.len() >= 4).then(|| input[..4].try_into().expect("4-byte selector")))
    }

    pub fn has_value(&self) -> Result<bool> {
        Ok(self.value_bytes()?.iter().any(|byte| *byte != 0))
    }

    pub fn is_call_type(&self) -> Result<bool> {
        let typ = self.typ()?;
        let flags = self.flags()?;
        Ok(matches!((typ, flags), (0, 0 | 1) | (1, _) | (2, _)))
    }
}

impl std::fmt::Debug for TraceRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceRef")
            .field("block_num", &self.block_num)
            .field("tx_idx", &self.tx_idx)
            .field("trace_idx", &self.trace_idx)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct CallFrameView<'a> {
    pub frame_bytes: &'a [u8],
    layout: TraceLayout,
}

impl<'a> CallFrameView<'a> {
    pub fn new(frame_bytes: &'a [u8]) -> Result<Self> {
        let layout = parse_trace_layout(frame_bytes)?;
        Ok(Self {
            frame_bytes,
            layout,
        })
    }

    pub fn typ(&self) -> Result<u8> {
        decode_u8(field_at(
            self.frame_bytes,
            self.layout.typ_field_start as usize,
        )?)
    }

    pub fn flags(&self) -> Result<u64> {
        decode_u64(field_at(
            self.frame_bytes,
            self.layout.flags_field_start as usize,
        )?)
    }

    pub fn from_addr(&self) -> Result<&'a Address20> {
        decode_address(field_at(
            self.frame_bytes,
            self.layout.from_field_start as usize,
        )?)
    }

    pub fn to_addr(&self) -> Result<Option<&'a Address20>> {
        decode_optional_address(field_at(
            self.frame_bytes,
            self.layout.to_field_start as usize,
        )?)
    }

    pub fn value_bytes(&self) -> Result<&'a [u8]> {
        decode_payload_bytes(field_at(
            self.frame_bytes,
            self.layout.value_field_start as usize,
        )?)
    }

    pub fn gas(&self) -> Result<u64> {
        decode_u64(field_at(
            self.frame_bytes,
            self.layout.gas_field_start as usize,
        )?)
    }

    pub fn gas_used(&self) -> Result<u64> {
        decode_u64(field_at(
            self.frame_bytes,
            self.layout.gas_used_field_start as usize,
        )?)
    }

    pub fn input(&self) -> Result<&'a [u8]> {
        decode_payload_bytes(field_at(
            self.frame_bytes,
            self.layout.input_field_start as usize,
        )?)
    }

    pub fn output(&self) -> Result<&'a [u8]> {
        decode_payload_bytes(field_at(
            self.frame_bytes,
            self.layout.output_field_start as usize,
        )?)
    }

    pub fn status(&self) -> Result<u8> {
        decode_u8(field_at(
            self.frame_bytes,
            self.layout.status_field_start as usize,
        )?)
    }

    pub fn depth(&self) -> Result<u64> {
        decode_u64(field_at(
            self.frame_bytes,
            self.layout.depth_field_start as usize,
        )?)
    }

    pub fn selector(&self) -> Result<Option<&'a Selector4>> {
        if !self.is_call_type()? {
            return Ok(None);
        }
        let input = self.input()?;
        Ok((input.len() >= 4).then(|| input[..4].try_into().expect("4-byte selector")))
    }

    pub fn has_value(&self) -> Result<bool> {
        Ok(self.value_bytes()?.iter().any(|byte| *byte != 0))
    }

    pub fn is_call_type(&self) -> Result<bool> {
        let typ = self.typ()?;
        let flags = self.flags()?;
        Ok(matches!((typ, flags), (0, 0 | 1) | (1, _) | (2, _)))
    }
}

fn parse_trace_layout(frame_bytes: &[u8]) -> Result<TraceLayout> {
    let typ_start = trace_payload_start(frame_bytes)?;
    let flags_start = next_field_start(frame_bytes, typ_start)?;
    let from_start = next_field_start(frame_bytes, flags_start)?;
    let to_start = next_field_start(frame_bytes, from_start)?;
    let value_start = next_field_start(frame_bytes, to_start)?;
    let gas_start = next_field_start(frame_bytes, value_start)?;
    let gas_used_start = next_field_start(frame_bytes, gas_start)?;
    let input_start = next_field_start(frame_bytes, gas_used_start)?;
    let output_start = next_field_start(frame_bytes, input_start)?;
    let status_start = next_field_start(frame_bytes, output_start)?;
    let depth_start = next_field_start(frame_bytes, status_start)?;
    let _end = next_field_start(frame_bytes, depth_start)?;

    Ok(TraceLayout {
        typ_field_start: u32::try_from(typ_start)
            .map_err(|_| Error::Decode("trace typ field start overflow"))?,
        flags_field_start: u32::try_from(flags_start)
            .map_err(|_| Error::Decode("trace flags field start overflow"))?,
        from_field_start: u32::try_from(from_start)
            .map_err(|_| Error::Decode("trace from field start overflow"))?,
        to_field_start: u32::try_from(to_start)
            .map_err(|_| Error::Decode("trace to field start overflow"))?,
        value_field_start: u32::try_from(value_start)
            .map_err(|_| Error::Decode("trace value field start overflow"))?,
        gas_field_start: u32::try_from(gas_start)
            .map_err(|_| Error::Decode("trace gas field start overflow"))?,
        gas_used_field_start: u32::try_from(gas_used_start)
            .map_err(|_| Error::Decode("trace gas_used field start overflow"))?,
        input_field_start: u32::try_from(input_start)
            .map_err(|_| Error::Decode("trace input field start overflow"))?,
        output_field_start: u32::try_from(output_start)
            .map_err(|_| Error::Decode("trace output field start overflow"))?,
        status_field_start: u32::try_from(status_start)
            .map_err(|_| Error::Decode("trace status field start overflow"))?,
        depth_field_start: u32::try_from(depth_start)
            .map_err(|_| Error::Decode("trace depth field start overflow"))?,
    })
}

fn trace_payload_start(frame_bytes: &[u8]) -> Result<usize> {
    let mut buf = frame_bytes;
    let PayloadView::List(fields) =
        Header::decode_raw(&mut buf).map_err(|_| Error::Decode("invalid trace call frame rlp"))?
    else {
        return Err(Error::Decode("trace call frame must be an rlp list"));
    };
    if !buf.is_empty() {
        return Err(Error::Decode("trace call frame has trailing bytes"));
    }
    if fields.len() != CALL_FRAME_FIELD_COUNT {
        return Err(Error::Decode("unexpected trace call frame field count"));
    }
    let first_field = fields
        .first()
        .ok_or(Error::Decode("unexpected trace call frame field count"))?;
    usize::try_from(byte_offset_in(frame_bytes, first_field))
        .map_err(|_| Error::Decode("trace field offset overflow"))
}

fn field_at(bytes: &[u8], start: usize) -> Result<&[u8]> {
    let mut buf = bytes
        .get(start..)
        .ok_or(Error::Decode("trace field start out of bounds"))?;
    Header::decode_raw(&mut buf).map_err(|_| Error::Decode("invalid trace field"))?;
    let end = bytes.len().saturating_sub(buf.len());
    bytes
        .get(start..end)
        .ok_or(Error::Decode("invalid trace field"))
}

fn next_field_start(bytes: &[u8], start: usize) -> Result<usize> {
    let mut buf = bytes
        .get(start..)
        .ok_or(Error::Decode("trace field start out of bounds"))?;
    Header::decode_raw(&mut buf).map_err(|_| Error::Decode("invalid trace field"))?;
    Ok(bytes.len().saturating_sub(buf.len()))
}

fn decode_u8(field: &[u8]) -> Result<u8> {
    let mut buf = field;
    let value = u8::decode(&mut buf).map_err(|_| Error::Decode("invalid u8 trace field"))?;
    if !buf.is_empty() {
        return Err(Error::Decode("trace field has trailing bytes"));
    }
    Ok(value)
}

fn decode_u64(field: &[u8]) -> Result<u64> {
    let mut buf = field;
    let value = u64::decode(&mut buf).map_err(|_| Error::Decode("invalid u64 trace field"))?;
    if !buf.is_empty() {
        return Err(Error::Decode("trace field has trailing bytes"));
    }
    Ok(value)
}

fn decode_payload_bytes(field: &[u8]) -> Result<&[u8]> {
    let mut buf = field;
    let payload = Header::decode_bytes(&mut buf, false)
        .map_err(|_| Error::Decode("invalid trace payload bytes"))?;
    if !buf.is_empty() {
        return Err(Error::Decode("trace payload field has trailing bytes"));
    }
    Ok(payload)
}

fn decode_address(field: &[u8]) -> Result<&Address20> {
    let payload = decode_payload_bytes(field)?;
    payload
        .try_into()
        .map_err(|_| Error::Decode("trace address must be 20 bytes"))
}

fn decode_optional_address(field: &[u8]) -> Result<Option<&Address20>> {
    let payload = decode_payload_bytes(field)?;
    if payload.is_empty() {
        return Ok(None);
    }
    payload
        .try_into()
        .map(Some)
        .map_err(|_| Error::Decode("trace optional address must be 20 bytes when present"))
}

#[cfg(test)]
mod tests {
    use super::CallFrameView;
    use alloy_rlp::Encodable;

    fn encode_field<T: Encodable>(value: T) -> Vec<u8> {
        let mut out = Vec::new();
        value.encode(&mut out);
        out
    }

    fn encode_bytes(value: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        value.encode(&mut out);
        out
    }

    fn encode_frame(
        typ: u8,
        flags: u64,
        from: [u8; 20],
        to: Option<[u8; 20]>,
        value: &[u8],
        gas: u64,
        gas_used: u64,
        input: &[u8],
        output: &[u8],
        status: u8,
        depth: u64,
    ) -> Vec<u8> {
        let fields = vec![
            encode_field(typ),
            encode_field(flags),
            encode_bytes(&from),
            encode_bytes(to.as_ref().map(<[u8; 20]>::as_slice).unwrap_or(&[])),
            encode_bytes(value),
            encode_field(gas),
            encode_field(gas_used),
            encode_bytes(input),
            encode_bytes(output),
            encode_field(status),
            encode_field(depth),
        ];
        let mut out = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: fields.iter().map(Vec::len).sum(),
        }
        .encode(&mut out);
        for field in fields {
            out.extend_from_slice(&field);
        }
        out
    }

    #[test]
    fn call_frame_view_accessors_work() {
        let frame = encode_frame(
            0,
            0,
            [1; 20],
            Some([2; 20]),
            &[0, 5],
            100,
            80,
            &[0xaa, 0xbb, 0xcc, 0xdd, 0xee],
            &[9, 8],
            1,
            0,
        );
        let view = CallFrameView::new(&frame).expect("frame view");

        assert_eq!(view.typ().unwrap(), 0);
        assert_eq!(view.flags().unwrap(), 0);
        assert_eq!(view.from_addr().unwrap(), &[1; 20]);
        assert_eq!(view.to_addr().unwrap().unwrap(), &[2; 20]);
        assert_eq!(view.value_bytes().unwrap(), &[0, 5]);
        assert!(view.has_value().unwrap());
        assert_eq!(view.selector().unwrap().unwrap(), &[0xaa, 0xbb, 0xcc, 0xdd]);
        assert_eq!(view.depth().unwrap(), 0);
    }
}
