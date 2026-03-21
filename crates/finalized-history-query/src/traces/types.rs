#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Trace {
    pub tx_idx: u32,
    pub trace_idx: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TraceStartupState;
