//! The part of a flow id that tells flows of one tree apart.

use rand::Rng;

/// Number of a flow within its tree.
///
/// Zero is reserved for the root, which lets anyone recognise the root flow
/// from its id alone, without looking for the split that created a child.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Subflow(u32);

impl Subflow {
    /// The root flow of a tree.
    pub const ROOT: Subflow = Subflow(0);

    /// Mints a random subflow for a child of this one.
    ///
    /// Random rather than sequential because children are split off in
    /// different processes with no shared counter. Never returns
    /// [`Self::ROOT`] or the parent itself: a split record whose child equals
    /// its parent would make a flow its own child, and a walk down the tree
    /// would never end. Clashes with siblings or more distant flows cannot be
    /// ruled out without shared state, and are left to the size of the space.
    pub fn mint_child<R: Rng + ?Sized>(self, rng: &mut R) -> Self {
        loop {
            let child = Subflow(rng.next_u32());
            if !child.is_root() && child != self {
                return child;
            }
        }
    }

    /// Whether this is the root flow of its tree.
    pub fn is_root(self) -> bool {
        self == Self::ROOT
    }
}

impl From<u32> for Subflow {
    fn from(value: u32) -> Self {
        Subflow(value)
    }
}

impl From<Subflow> for u32 {
    fn from(value: Subflow) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use rand::{SeedableRng, TryRng, rngs::StdRng};

    use super::*;

    /// Yields the given values in order, so a test can offer the forbidden
    /// subflows first.
    struct ScriptedRng(std::vec::IntoIter<u32>);

    impl TryRng for ScriptedRng {
        type Error = Infallible;

        fn try_next_u32(&mut self) -> Result<u32, Infallible> {
            Ok(self.0.next().expect("script ran out of values"))
        }

        fn try_next_u64(&mut self) -> Result<u64, Infallible> {
            unimplemented!("subflows draw u32 only")
        }

        fn try_fill_bytes(&mut self, _dst: &mut [u8]) -> Result<(), Infallible> {
            unimplemented!("subflows draw u32 only")
        }
    }

    #[test]
    fn child_skips_root_and_parent() {
        let parent = Subflow::from(7);
        let mut rng = ScriptedRng(vec![0, 7, 0, 8].into_iter());
        assert_eq!(parent.mint_child(&mut rng), Subflow::from(8));
    }

    #[test]
    fn children_are_never_root_nor_parent() {
        let mut rng = StdRng::seed_from_u64(7);
        let parent = Subflow::ROOT.mint_child(&mut rng);
        assert!((0..10_000).all(|_| {
            let child = parent.mint_child(&mut rng);
            !child.is_root() && child != parent
        }));
    }

    #[test]
    fn zero_is_root() {
        assert!(Subflow::from(0).is_root());
        assert!(!Subflow::from(1).is_root());
    }
}
