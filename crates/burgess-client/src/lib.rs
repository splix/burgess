//! Publishes an application's internal state to Burgess.
//!
//! Applications link this library to mint flow ids and emit state, split and
//! reference records to Kafka. Capture is best effort. A failed publish must
//! never block or fail the application it observes, because the point of
//! capturing state is to debug that application, not to become a new way for it
//! to fail.
