use crate::store::traits::TableId;

pub const PUBLICATION_STATE_TABLE: TableId = TableId::new("publication_state");
pub const PUBLICATION_STATE_SUFFIX: &[u8] = b"state";
