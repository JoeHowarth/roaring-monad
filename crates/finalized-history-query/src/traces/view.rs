use alloy_rlp::{Decodable, Header, PayloadView};

use crate::error::{Error, Result};
use crate::traces::types::{Address20, Selector4};

const CALL_FRAME_FIELD_COUNT: usize = 11;

#[derive(Debug, Clone)]
pub struct CallFrameView<'a> {
    frame_bytes: &'a [u8],
    fields: Vec<&'a [u8]>,
}

impl<'a> CallFrameView<'a> {
    pub fn new(frame_bytes: &'a [u8]) -> Result<Self> {
        let mut buf = frame_bytes;
        let PayloadView::List(fields) = Header::decode_raw(&mut buf)
            .map_err(|_| Error::Decode("invalid trace call frame rlp"))?
        else {
            return Err(Error::Decode("trace call frame must be an rlp list"));
        };
        if !buf.is_empty() {
            return Err(Error::Decode("trace call frame has trailing bytes"));
        }
        if fields.len() != CALL_FRAME_FIELD_COUNT {
            return Err(Error::Decode("unexpected trace call frame field count"));
        }
        Ok(Self {
            frame_bytes,
            fields,
        })
    }

    pub fn frame_bytes(&self) -> &'a [u8] {
        self.frame_bytes
    }

    pub fn typ(&self) -> Result<u8> {
        decode_u8(self.field(0)?)
    }

    pub fn flags(&self) -> Result<u64> {
        decode_u64(self.field(1)?)
    }

    pub fn from_addr(&self) -> Result<&'a Address20> {
        decode_address(self.field(2)?)
    }

    pub fn to_addr(&self) -> Result<Option<&'a Address20>> {
        decode_optional_address(self.field(3)?)
    }

    pub fn value_bytes(&self) -> Result<&'a [u8]> {
        decode_payload_bytes(self.field(4)?)
    }

    pub fn gas(&self) -> Result<u64> {
        decode_u64(self.field(5)?)
    }

    pub fn gas_used(&self) -> Result<u64> {
        decode_u64(self.field(6)?)
    }

    pub fn input(&self) -> Result<&'a [u8]> {
        decode_payload_bytes(self.field(7)?)
    }

    pub fn output(&self) -> Result<&'a [u8]> {
        decode_payload_bytes(self.field(8)?)
    }

    pub fn status(&self) -> Result<u8> {
        decode_u8(self.field(9)?)
    }

    pub fn depth(&self) -> Result<u64> {
        decode_u64(self.field(10)?)
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

    fn field(&self, index: usize) -> Result<&'a [u8]> {
        self.fields
            .get(index)
            .copied()
            .ok_or(Error::Decode("trace call frame field out of bounds"))
    }
}

#[derive(Debug, Clone)]
pub struct IteratedCallFrame<'a> {
    pub tx_idx: u32,
    pub trace_idx: u32,
    pub byte_offset: u64,
    pub byte_length: u64,
    pub view: CallFrameView<'a>,
}

pub struct BlockTraceIter<'a> {
    blob: &'a [u8],
    txs: Vec<&'a [u8]>,
    tx_idx: usize,
    trace_idx_in_tx: u32,
    current_frames: Vec<&'a [u8]>,
    current_frame_idx: usize,
}

impl<'a> BlockTraceIter<'a> {
    pub fn new(blob: &'a [u8]) -> Result<Self> {
        let txs = if blob.is_empty() {
            Vec::new()
        } else {
            let mut buf = blob;
            match Header::decode_raw(&mut buf)
                .map_err(|_| Error::Decode("invalid block trace rlp"))?
            {
                PayloadView::List(items) => {
                    if !buf.is_empty() {
                        return Err(Error::Decode("block trace rlp has trailing bytes"));
                    }
                    items
                }
                PayloadView::String(_) => {
                    return Err(Error::Decode("block trace blob must be an rlp list"));
                }
            }
        };

        Ok(Self {
            blob,
            txs,
            tx_idx: 0,
            trace_idx_in_tx: 0,
            current_frames: Vec::new(),
            current_frame_idx: 0,
        })
    }
}

impl<'a> Iterator for BlockTraceIter<'a> {
    type Item = Result<IteratedCallFrame<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_frame_idx < self.current_frames.len() {
                let frame_bytes = self.current_frames[self.current_frame_idx];
                let trace_idx = self.trace_idx_in_tx;
                self.current_frame_idx += 1;
                self.trace_idx_in_tx = self.trace_idx_in_tx.saturating_add(1);

                let byte_offset = crate::core::offsets::byte_offset_in(self.blob, frame_bytes);
                let byte_length = frame_bytes.len() as u64;
                let tx_idx = self.tx_idx.saturating_sub(1) as u32;
                return Some(
                    CallFrameView::new(frame_bytes).map(|view| IteratedCallFrame {
                        tx_idx,
                        trace_idx,
                        byte_offset,
                        byte_length,
                        view,
                    }),
                );
            }

            let tx_bytes = self.txs.get(self.tx_idx).copied()?;
            self.tx_idx += 1;
            self.trace_idx_in_tx = 0;

            let mut tx_buf = tx_bytes;
            self.current_frames = match Header::decode_raw(&mut tx_buf)
                .map_err(|_| Error::Decode("invalid transaction trace list"))
            {
                Ok(PayloadView::List(items)) => {
                    if !tx_buf.is_empty() {
                        return Some(Err(Error::Decode(
                            "transaction trace list has trailing bytes",
                        )));
                    }
                    items
                }
                Ok(PayloadView::String(_)) => {
                    return Some(Err(Error::Decode("transaction traces must be an rlp list")));
                }
                Err(err) => return Some(Err(err)),
            };
            self.current_frame_idx = 0;
        }
    }
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
    use super::{BlockTraceIter, CallFrameView};
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

    fn encode_block(txs: Vec<Vec<Vec<u8>>>) -> Vec<u8> {
        let tx_blobs = txs
            .into_iter()
            .map(|frames| {
                let payload_length = frames.iter().map(Vec::len).sum();
                let mut tx = Vec::new();
                alloy_rlp::Header {
                    list: true,
                    payload_length,
                }
                .encode(&mut tx);
                for frame in frames {
                    tx.extend_from_slice(&frame);
                }
                tx
            })
            .collect::<Vec<_>>();
        let payload_length = tx_blobs.iter().map(Vec::len).sum();
        let mut out = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length,
        }
        .encode(&mut out);
        for tx in tx_blobs {
            out.extend_from_slice(&tx);
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

    #[test]
    fn block_trace_iter_preserves_tx_and_trace_order() {
        let block = encode_block(vec![
            vec![encode_frame(
                0,
                0,
                [1; 20],
                Some([2; 20]),
                &[],
                1,
                1,
                &[],
                &[],
                1,
                0,
            )],
            vec![
                encode_frame(1, 0, [3; 20], None, &[7], 2, 2, &[], &[], 1, 1),
                encode_frame(2, 0, [4; 20], Some([5; 20]), &[], 3, 3, &[], &[], 1, 2),
            ],
        ]);

        let frames = BlockTraceIter::new(&block)
            .expect("iter")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect frames");

        assert_eq!(frames.len(), 3);
        assert_eq!((frames[0].tx_idx, frames[0].trace_idx), (0, 0));
        assert_eq!((frames[1].tx_idx, frames[1].trace_idx), (1, 0));
        assert_eq!((frames[2].tx_idx, frames[2].trace_idx), (1, 1));
        assert!(frames[1].byte_offset > frames[0].byte_offset);
    }
}
