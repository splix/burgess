//! Telling apart the errors librdkafka hands the consumer.
//!
//! librdkafka reports broker hiccups through the same channel as real
//! failures. Treating every report as a failure would stop the writer at
//! each broker restart; ignoring them all would hide real ones.

use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;

/// What an error from the consumer means for the writer.
#[derive(Debug)]
pub(super) enum ConsumerError {
    /// A condition librdkafka recovers from by itself, such as a lost broker
    /// connection.
    Temporary(RDKafkaErrorCode),
    /// A position being read is no longer held, because retention removed
    /// the records. librdkafka stops fetching the partition until it is
    /// assigned again.
    PositionLost,
    /// Anything else; the consumer cannot go on.
    Fatal(KafkaError),
}

impl From<KafkaError> for ConsumerError {
    fn from(error: KafkaError) -> Self {
        match error {
            KafkaError::MessageConsumption(RDKafkaErrorCode::AutoOffsetReset) => {
                ConsumerError::PositionLost
            }
            KafkaError::MessageConsumption(code) => ConsumerError::Temporary(code),
            other => ConsumerError::Fatal(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_trouble_is_temporary() {
        for code in [
            RDKafkaErrorCode::AllBrokersDown,
            RDKafkaErrorCode::BrokerTransportFailure,
        ] {
            assert!(
                matches!(
                    ConsumerError::from(KafkaError::MessageConsumption(code)),
                    ConsumerError::Temporary(reported) if reported == code
                ),
                "{code}"
            );
        }
    }

    #[test]
    fn offset_reset_means_a_lost_position() {
        let error = KafkaError::MessageConsumption(RDKafkaErrorCode::AutoOffsetReset);
        assert!(matches!(
            ConsumerError::from(error),
            ConsumerError::PositionLost
        ));
    }

    #[test]
    fn fatal_errors_stay_fatal() {
        let error = KafkaError::MessageConsumptionFatal(RDKafkaErrorCode::Fatal);
        assert!(matches!(
            ConsumerError::from(error),
            ConsumerError::Fatal(_)
        ));
    }
}
