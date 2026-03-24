use crate::core::state::BlockRecordSpec;
use crate::kernel::table_specs::BlobTableSpec;
use crate::kernel::table_specs::{PointTableSpec, ScannableTableSpec};
use crate::logs::table_specs::{
    BitmapByBlockSpec, BitmapPageBlobSpec, BitmapPageMetaSpec, BlockHashIndexSpec,
    BlockLogBlobSpec, BlockLogHeaderSpec, LogDirBucketSpec, LogDirByBlockSpec, LogDirSubBucketSpec,
    OpenBitmapPageSpec,
};
use crate::store::publication::PUBLICATION_STATE_TABLE;
use crate::store::traits::{BlobTableId, ScannableTableId, TableId};
use crate::traces::table_specs::{
    BlockTraceBlobSpec, BlockTraceHeaderSpec, TraceBitmapByBlockSpec, TraceBitmapPageBlobSpec,
    TraceBitmapPageMetaSpec, TraceDirBucketSpec, TraceDirByBlockSpec, TraceDirSubBucketSpec,
    TraceOpenBitmapPageSpec,
};

pub const RUNTIME_POINT_TABLES: [TableId; 10] = [
    BlockRecordSpec::TABLE,
    BlockLogHeaderSpec::TABLE,
    BlockHashIndexSpec::TABLE,
    LogDirBucketSpec::TABLE,
    LogDirSubBucketSpec::TABLE,
    BitmapPageMetaSpec::TABLE,
    BlockTraceHeaderSpec::TABLE,
    TraceDirBucketSpec::TABLE,
    TraceDirSubBucketSpec::TABLE,
    TraceBitmapPageMetaSpec::TABLE,
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
    LogDirByBlockSpec::TABLE,
    BitmapByBlockSpec::TABLE,
    OpenBitmapPageSpec::TABLE,
    TraceDirByBlockSpec::TABLE,
    TraceBitmapByBlockSpec::TABLE,
    TraceOpenBitmapPageSpec::TABLE,
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
    BlockTraceBlobSpec::TABLE,
    TraceBitmapPageBlobSpec::TABLE,
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
