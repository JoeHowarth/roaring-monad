use crate::error::Result;
use crate::family::FamilyStates;
use crate::ingest::open_pages::repair_sealed_open_bitmap_pages;
use crate::tables::Tables;
use crate::store::traits::{BlobStore, MetaStore};

/// Repair mutable ingest state that may have been left behind by a previous
/// writer session before resuming ingest from the published head.
pub async fn repair_after_ownership_transition<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    family_states: &FamilyStates,
) -> Result<()> {
    repair_sealed_open_bitmap_pages(
        tables,
        family_states.logs.next_log_id.get(),
        family_states.traces.next_trace_id.get(),
    )
    .await
}
