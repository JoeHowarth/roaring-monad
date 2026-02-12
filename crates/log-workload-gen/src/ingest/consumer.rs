use crate::ingest::validator::Validator;
use crate::types::{DatasetSummary, Message};

pub fn consume_messages(messages: impl IntoIterator<Item = Message>) -> DatasetSummary {
    let mut validator = Validator::new();

    for message in messages {
        match message {
            Message::ChainEvent(event) => {
                if let Err(reason) = validator.accept(&event) {
                    return validator.summary_with_validity(false, Some(reason));
                }
            }
            Message::EndOfStream { expected_end_block } => {
                if validator.end_block() != Some(expected_end_block) {
                    return validator
                        .summary_with_validity(false, Some("end_of_stream_mismatch".to_string()));
                }
                return validator.summary_with_validity(true, None);
            }
        }
    }

    validator.summary_with_validity(false, Some("channel_closed_unexpectedly".to_string()))
}
