//! The values with the smallest keys of all that were offered.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::num::NonZeroUsize;

/// Keeps the `size` values with the smallest keys of all that it is
/// offered, and drops the others, thus it holds no more than `size` values
/// at a time.
#[derive(Debug)]
pub(crate) struct Smallest<K, T> {
    size: NonZeroUsize,
    /// A max-heap, thus the largest key kept is the one to drop first.
    heap: BinaryHeap<Entry<K, T>>,
}

#[derive(Debug)]
struct Entry<K, T> {
    key: K,
    value: T,
}

impl<K: Ord, T> Smallest<K, T> {
    pub(crate) fn new(size: NonZeroUsize) -> Self {
        Smallest {
            size,
            heap: BinaryHeap::new(),
        }
    }

    /// Keeps `value` if its key is among the smallest so far.
    pub(crate) fn offer(&mut self, key: K, value: T) {
        if self.heap.len() < self.size.get() {
            self.heap.push(Entry { key, value });
        } else if let Some(mut largest) = self.heap.peek_mut()
            && key < largest.key
        {
            *largest = Entry { key, value };
        }
    }

    /// The largest key kept when the heap is full: a value with a larger
    /// key is not kept. `None` while there is room.
    pub(crate) fn bar(&self) -> Option<&K> {
        (self.heap.len() == self.size.get())
            .then(|| self.heap.peek().map(|largest| &largest.key))
            .flatten()
    }

    /// The values kept, smallest key first.
    pub(crate) fn into_sorted(self) -> Vec<T> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|entry| entry.value)
            .collect()
    }
}

impl<K: Ord, T> PartialEq for Entry<K, T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K: Ord, T> Eq for Entry<K, T> {}

impl<K: Ord, T> PartialOrd for Entry<K, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord, T> Ord for Entry<K, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn smallest(size: usize) -> Smallest<i64, char> {
        Smallest::new(NonZeroUsize::new(size).unwrap())
    }

    #[test]
    fn keeps_the_smallest_keys_in_order() {
        let mut kept = smallest(3);
        for (key, value) in [(5, 'e'), (1, 'a'), (4, 'd'), (2, 'b'), (3, 'c')] {
            kept.offer(key, value);
        }
        assert_eq!(kept.into_sorted(), ['a', 'b', 'c']);
    }

    #[test]
    fn the_bar_is_known_only_when_full() {
        let mut kept = smallest(2);
        kept.offer(7, 'x');
        assert_eq!(kept.bar(), None);
        kept.offer(3, 'y');
        assert_eq!(kept.bar(), Some(&7));
        kept.offer(5, 'z');
        assert_eq!(kept.bar(), Some(&5));
    }

    #[test]
    fn a_key_equal_to_the_bar_is_not_kept() {
        let mut kept = smallest(1);
        kept.offer(1, 'a');
        kept.offer(1, 'b');
        assert_eq!(kept.into_sorted(), ['a']);
    }
}
