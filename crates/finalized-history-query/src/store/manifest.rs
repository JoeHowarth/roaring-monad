use crate::core::state::BLOCK_RECORD_TABLE;
use crate::kernel::table_specs::BlobTableSpec;
use crate::logs::keys::{
    BITMAP_BY_BLOCK_TABLE, BITMAP_PAGE_META_TABLE, BLOCK_HASH_INDEX_TABLE, BLOCK_LOG_HEADER_TABLE,
    LOG_DIR_BUCKET_TABLE, LOG_DIR_BY_BLOCK_TABLE, LOG_DIR_SUB_BUCKET_TABLE, OPEN_BITMAP_PAGE_TABLE,
};
use crate::logs::table_specs::{BitmapPageBlobSpec, BlockLogBlobSpec};
use crate::store::publication::PUBLICATION_STATE_TABLE;
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};
use crate::traces::keys::{
    BLOCK_TRACE_BLOB_TABLE, BLOCK_TRACE_HEADER_TABLE, TRACE_BITMAP_BY_BLOCK_TABLE,
    TRACE_BITMAP_PAGE_BLOB_TABLE, TRACE_BITMAP_PAGE_META_TABLE, TRACE_DIR_BUCKET_TABLE,
    TRACE_DIR_BY_BLOCK_TABLE, TRACE_DIR_SUB_BUCKET_TABLE, TRACE_OPEN_BITMAP_PAGE_TABLE,
};

pub const RUNTIME_POINT_TABLES: [TableId; 10] = [
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

pub const REQUIRED_POINT_TABLES: [TableId; 11] = [
    PUBLICATION_STATE_TABLE,
    RUNTIME_POINT_TABLES[0],
    RUNTIME_POINT_TABLES[1],
    RUNTIME_POINT_TABLES[2],
    RUNTIME_POINT_TABLES[3],
    RUNTIME_POINT_TABLES[4],
    RUNTIME_POINT_TABLES[5],
    RUNTIME_POINT_TABLES[6],
    RUNTIME_POINT_TABLES[7],
    RUNTIME_POINT_TABLES[8],
    RUNTIME_POINT_TABLES[9],
];

pub const RUNTIME_SCANNABLE_TABLES: [ScannableTableId; 6] = [
    LOG_DIR_BY_BLOCK_TABLE,
    BITMAP_BY_BLOCK_TABLE,
    OPEN_BITMAP_PAGE_TABLE,
    TRACE_DIR_BY_BLOCK_TABLE,
    TRACE_BITMAP_BY_BLOCK_TABLE,
    TRACE_OPEN_BITMAP_PAGE_TABLE,
];

pub const REQUIRED_SCANNABLE_TABLES: [ScannableTableId; 6] = [
    RUNTIME_SCANNABLE_TABLES[0],
    RUNTIME_SCANNABLE_TABLES[1],
    RUNTIME_SCANNABLE_TABLES[2],
    RUNTIME_SCANNABLE_TABLES[3],
    RUNTIME_SCANNABLE_TABLES[4],
    RUNTIME_SCANNABLE_TABLES[5],
];

pub const RUNTIME_BLOB_TABLES: [BlobTableId; 4] = [
    BlockLogBlobSpec::TABLE,
    BitmapPageBlobSpec::TABLE,
    BLOCK_TRACE_BLOB_TABLE,
    TRACE_BITMAP_PAGE_BLOB_TABLE,
];

pub const REQUIRED_BLOB_TABLES: [BlobTableId; 4] = [
    RUNTIME_BLOB_TABLES[0],
    RUNTIME_BLOB_TABLES[1],
    RUNTIME_BLOB_TABLES[2],
    RUNTIME_BLOB_TABLES[3],
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn required_storage_tables_are_unique() {
        assert_eq!(
            REQUIRED_POINT_TABLES.len(),
            REQUIRED_POINT_TABLES
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
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
            REQUIRED_BLOB_TABLES
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
        );
    }
}
