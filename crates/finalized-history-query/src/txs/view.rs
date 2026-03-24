use crate::error::Result;
use crate::family::Hash32;
use crate::txs::types::{Address20, Selector4};

#[derive(Debug, Clone, Copy)]
pub enum TxView<'a> {
    Legacy(LegacyTxView<'a>),
    Eip2930(Eip2930TxView<'a>),
    Eip1559(Eip1559TxView<'a>),
    Eip4844(Eip4844TxView<'a>),
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
