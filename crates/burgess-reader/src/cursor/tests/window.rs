use jiff::{SignedDuration, Timestamp};

use super::at;
use crate::cursor::{Window, WindowError};

#[test]
fn a_window_can_be_one_instant() {
    let window = Window::new(Some(at(1_000)), Some(at(1_000))).unwrap();
    assert_eq!(window.since(), Some(at(1_000)));
    assert_eq!(window.until(), Some(at(1_000)));
}

#[test]
fn a_window_that_ends_before_it_starts_is_refused() {
    assert_eq!(
        Window::new(Some(at(1_001)), Some(at(1_000))),
        Err(WindowError::Reversed {
            since: at(1_001),
            until: at(1_000),
        })
    );
}

/// A cursor keeps milliseconds. A part of one would be lost, and the next
/// page would read another window than the first page.
#[test]
fn a_time_with_a_part_of_a_millisecond_is_refused() {
    let part = at(1_000) + SignedDuration::from_micros(400);
    assert_eq!(
        Window::new(Some(part), None),
        Err(WindowError::NotWholeMs(part))
    );
    assert_eq!(
        Window::new(None, Some(part)),
        Err(WindowError::NotWholeMs(part))
    );
}

#[test]
fn a_time_that_cannot_bound_a_lookup_is_refused() {
    assert!(matches!(
        Window::new(Some(Timestamp::MIN), None),
        Err(WindowError::Time(_))
    ));
}
