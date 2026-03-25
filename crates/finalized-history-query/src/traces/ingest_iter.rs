use alloy_rlp::{Header, PayloadView};

use crate::error::{Error, Result};
use crate::traces::view::CallFrameView;

#[derive(Debug, Clone)]
pub struct IteratedCallFrame<'a> {
    pub tx_idx: u32,
    pub trace_idx: u32,
    pub view: CallFrameView<'a>,
}

pub struct BlockTraceIter<'a> {
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

                let tx_idx = self.tx_idx.saturating_sub(1) as u32;
                return Some(
                    CallFrameView::new(frame_bytes).map(|view| IteratedCallFrame {
                        tx_idx,
                        trace_idx,
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

#[cfg(test)]
mod tests {
    use super::BlockTraceIter;
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

    #[derive(Clone, Copy)]
    struct TraceFrameParts<'a> {
        typ: u8,
        flags: u64,
        from: [u8; 20],
        to: Option<[u8; 20]>,
        value: &'a [u8],
        gas: u64,
        gas_used: u64,
        input: &'a [u8],
        output: &'a [u8],
        status: u8,
        depth: u64,
    }

    fn encode_frame(parts: TraceFrameParts<'_>) -> Vec<u8> {
        let fields = vec![
            encode_field(parts.typ),
            encode_field(parts.flags),
            encode_bytes(&parts.from),
            encode_bytes(parts.to.as_ref().map(<[u8; 20]>::as_slice).unwrap_or(&[])),
            encode_bytes(parts.value),
            encode_field(parts.gas),
            encode_field(parts.gas_used),
            encode_bytes(parts.input),
            encode_bytes(parts.output),
            encode_field(parts.status),
            encode_field(parts.depth),
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
    fn block_trace_iter_preserves_tx_and_trace_order() {
        let block = encode_block(vec![
            vec![encode_frame(TraceFrameParts {
                typ: 0,
                flags: 0,
                from: [1; 20],
                to: Some([2; 20]),
                value: &[],
                gas: 1,
                gas_used: 1,
                input: &[],
                output: &[],
                status: 1,
                depth: 0,
            })],
            vec![
                encode_frame(TraceFrameParts {
                    typ: 1,
                    flags: 0,
                    from: [3; 20],
                    to: None,
                    value: &[7],
                    gas: 2,
                    gas_used: 2,
                    input: &[],
                    output: &[],
                    status: 1,
                    depth: 1,
                }),
                encode_frame(TraceFrameParts {
                    typ: 2,
                    flags: 0,
                    from: [4; 20],
                    to: Some([5; 20]),
                    value: &[],
                    gas: 3,
                    gas_used: 3,
                    input: &[],
                    output: &[],
                    status: 1,
                    depth: 2,
                }),
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
    }
}
