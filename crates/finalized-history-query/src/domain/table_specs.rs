use crate::store::traits::{ScannableTableId, TableId};

pub trait PointTableSpec {
    const TABLE: TableId;
}

pub trait ScannableTableSpec {
    const TABLE: ScannableTableId;
}

pub trait BlobTableSpec {
    const TABLE: crate::store::traits::BlobTableId;
}

pub struct PublicationStateSpec;
impl PointTableSpec for PublicationStateSpec {
    const TABLE: TableId = TableId::new("publication_state");
}
impl PublicationStateSpec {
    pub const fn key() -> &'static [u8] {
        b"state"
    }
}
