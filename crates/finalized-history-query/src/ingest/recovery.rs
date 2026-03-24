use crate::error::Result;
use crate::family::FamilyStates;
use crate::ingest::authority::WriteContinuity;
use crate::ingest::open_pages::repair_sealed_open_bitmap_pages;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryDisposition {
    SkippedContinuousLease,
    RepairedAfterFreshAcquire,
    RepairedAfterReacquire,
}

/// Apply any writer-side recovery required before ingest continues from the
/// published head. Continuous leases skip recovery entirely; fresh acquisition
/// and reacquisition repair ownership-transition state idempotently.
pub async fn preflight_recovery<M: MetaStore, B: BlobStore>(
    continuity: WriteContinuity,
    tables: &Tables<M, B>,
    family_states: &FamilyStates,
) -> Result<RecoveryDisposition> {
    match continuity {
        WriteContinuity::Continuous => Ok(RecoveryDisposition::SkippedContinuousLease),
        WriteContinuity::Fresh => {
            repair_after_ownership_transition(tables, family_states).await?;
            Ok(RecoveryDisposition::RepairedAfterFreshAcquire)
        }
        WriteContinuity::Reacquired => {
            repair_after_ownership_transition(tables, family_states).await?;
            Ok(RecoveryDisposition::RepairedAfterReacquire)
        }
    }
}

async fn repair_after_ownership_transition<M: MetaStore, B: BlobStore>(
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
