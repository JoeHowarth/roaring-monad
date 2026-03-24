use crate::core::state::BLOCK_RECORD_TABLE;
use crate::logs::keys::{
    BITMAP_BY_BLOCK_TABLE, BITMAP_PAGE_META_TABLE, BLOCK_HASH_INDEX_TABLE, BLOCK_LOG_HEADER_TABLE,
    LOG_DIR_BUCKET_TABLE, LOG_DIR_BY_BLOCK_TABLE, LOG_DIR_SUB_BUCKET_TABLE, OPEN_BITMAP_PAGE_TABLE,
};
use crate::logs::table_specs::{BitmapPageBlobSpec, BlockLogBlobSpec};
use crate::kernel::table_specs::BlobTableSpec;
use crate::store::publication::PUBLICATION_STATE_TABLE;
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};
use crate::traces::keys::{
    BLOCK_TRACE_BLOB_TABLE, BLOCK_TRACE_HEADER_TABLE, TRACE_BITMAP_BY_BLOCK_TABLE,
    TRACE_BITMAP_PAGE_BLOB_TABLE, TRACE_BITMAP_PAGE_META_TABLE, TRACE_DIR_BUCKET_TABLE,
    TRACE_DIR_BY_BLOCK_TABLE, TRACE_DIR_SUB_BUCKET_TABLE, TRACE_OPEN_BITMAP_PAGE_TABLE,
};

pub const REQUIRED_POINT_TABLES: [TableId; 11] = [
    PUBLICATION_STATE_TABLE,
    BLOCK_RECORD_TABLE,
    BLOCK_LOG_HEADER_TABLE,
    BLOCK_HASH_INDEX_TABLE,
    LOG_DIR_BUCKET_TABLE,
    LOG_DIR_SUB_BUCKET_TABLE,
    BITMAP_PAGE_META_TABLE,
    BLOCK_TRACE_HEADER_TABLE,
    TRACE_DIR_BUCKET_TABLE,
    TRACE_DIR_SUB_BUCKET_TABLE,
    TRACE_BITMAP_PAGE_META_TABLE,
];

pub const REQUIRED_SCANNABLE_TABLES: [ScannableTableId; 6] = [
    LOG_DIR_BY_BLOCK_TABLE,
    BITMAP_BY_BLOCK_TABLE,
    OPEN_BITMAP_PAGE_TABLE,
    TRACE_DIR_BY_BLOCK_TABLE,
    TRACE_BITMAP_BY_BLOCK_TABLE,
    TRACE_OPEN_BITMAP_PAGE_TABLE,
];

pub const REQUIRED_BLOB_TABLES: [BlobTableId; 4] = [
    BlockLogBlobSpec::TABLE,
    BitmapPageBlobSpec::TABLE,
    BLOCK_TRACE_BLOB_TABLE,
    TRACE_BITMAP_PAGE_BLOB_TABLE,
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn required_storage_tables_are_unique() {
        assert_eq!(
            REQUIRED_POINT_TABLES.len(),
            REQUIRED_POINT_TABLES.iter().copied().collect::<BTreeSet<_>>().len()
        );
        assert_eq!(
            REQUIRED_SCANNABLE_TABLES.len(),
            REQUIRED_SCANNABLE_TABLES
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
        );
        assert_eq!(
            REQUIRED_BLOB_TABLES.len(),
            REQUIRED_BLOB_TABLES.iter().copied().collect::<BTreeSet<_>>().len()
        );
    }
}
