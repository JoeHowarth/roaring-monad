use crate::ingest::validator::{AcceptOutcome, Validator};
use crate::types::{ChainEvent, DatasetSummary, Message};

pub fn consume_messages(messages: impl IntoIterator<Item = Message>) -> DatasetSummary {
    consume_messages_with_events(messages).0
}

pub(crate) fn consume_messages_with_events(
    messages: impl IntoIterator<Item = Message>,
) -> (DatasetSummary, Vec<ChainEvent>) {
    let mut validator = Validator::new();
    let mut accepted_events = Vec::new();

    for message in messages {
        match message {
            Message::ChainEvent(event) => match validator.accept(&event) {
                Ok(AcceptOutcome::New) => accepted_events.push(event),
                Ok(AcceptOutcome::Duplicate) => {}
                Err(reason) => {
                    return (
                        validator.summary_with_validity(false, Some(reason)),
                        accepted_events,
                    );
                }
            },
            Message::EndOfStream { expected_end_block } => {
                if validator.end_block() != Some(expected_end_block) {
                    return (
                        validator.summary_with_validity(
                            false,
                            Some("end_of_stream_mismatch".to_string()),
                        ),
                        accepted_events,
                    );
                }
                return (validator.summary_with_validity(true, None), accepted_events);
            }
        }
    }

    (
        validator.summary_with_validity(false, Some("channel_closed_unexpectedly".to_string())),
        accepted_events,
    )
}
