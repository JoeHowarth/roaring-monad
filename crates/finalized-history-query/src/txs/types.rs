pub type Hash32 = [u8; 32];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Tx {
    pub tx_idx: u32,
    pub tx_hash: Hash32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TxStartupState;
