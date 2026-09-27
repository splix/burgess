//! The id every record carries.

use std::fmt;
use std::str::FromStr;

use jiff::Timestamp;
use rand::Rng;

use super::hex::{HexDigits, IdForm, IdParseError, write_hex};
use super::subflow::Subflow;
use super::tree_id::TreeId;

/// Identifies one flow: the [`TreeId`] of its tree followed by its
/// [`Subflow`], 16 bytes big-endian.
///
/// Ids are minted where the flow starts, with no coordination between
/// processes. Sorting by id groups a tree together and orders trees by start
/// time, which is the order the writer stores records in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FlowId {
    tree: TreeId,
    subflow: Subflow,
}

impl FlowId {
    /// Puts an id together from its parts.
    pub fn new(tree: TreeId, subflow: Subflow) -> Self {
        FlowId { tree, subflow }
    }

    /// Mints the root flow of a new tree, using the system clock and the
    /// thread-local random generator.
    pub fn new_root() -> Self {
        Self::mint_root(Timestamp::now(), &mut rand::rng())
    }

    /// Mints the root flow of a new tree started at `now`.
    pub fn mint_root<R: Rng + ?Sized>(now: Timestamp, rng: &mut R) -> Self {
        Self::new(TreeId::mint(now, rng), Subflow::ROOT)
    }

    /// Mints a child of this flow, using the thread-local random generator.
    pub fn new_child(&self) -> Self {
        self.mint_child(&mut rand::rng())
    }

    /// Mints a child of this flow.
    ///
    /// The child stays in its parent's tree. That is what lets one lookup by
    /// tree id return the whole execution, however far it spread.
    pub fn mint_child<R: Rng + ?Sized>(&self, rng: &mut R) -> Self {
        Self::new(self.tree, self.subflow.mint_child(rng))
    }

    /// The tree this flow belongs to.
    pub fn tree(&self) -> TreeId {
        self.tree
    }

    /// The flow's number within its tree.
    pub fn subflow(&self) -> Subflow {
        self.subflow
    }

    /// Whether this is the root flow of its tree.
    pub fn is_root(&self) -> bool {
        self.subflow.is_root()
    }

    /// When the root of this flow's tree was minted. See [`TreeId::started_at`].
    pub fn started_at(&self) -> Timestamp {
        self.tree.started_at()
    }

    /// The id as it is stored and sent.
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut bytes = [0; 16];
        bytes[..12].copy_from_slice(self.tree.as_bytes());
        bytes[12..].copy_from_slice(&u32::from(self.subflow).to_be_bytes());
        bytes
    }
}

impl From<[u8; 16]> for FlowId {
    fn from(bytes: [u8; 16]) -> Self {
        let mut tree = [0; 12];
        tree.copy_from_slice(&bytes[..12]);
        let mut subflow = [0; 4];
        subflow.copy_from_slice(&bytes[12..]);
        FlowId::new(tree.into(), u32::from_be_bytes(subflow).into())
    }
}

impl From<FlowId> for [u8; 16] {
    fn from(id: FlowId) -> Self {
        id.to_bytes()
    }
}

impl fmt::Display for FlowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(f, &self.to_bytes())
    }
}

impl FromStr for FlowId {
    type Err = IdParseError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let digits = HexDigits::parse(text)?;
        digits
            .to_bytes()
            .map(FlowId::from)
            .ok_or_else(|| digits.wrong_length(IdForm::Flow))
    }
}

#[cfg(test)]
mod tests {
    use rand::{SeedableRng, rngs::StdRng};

    use super::*;

    const BYTES: [u8; 16] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f,
    ];
    const TEXT: &str = "000102030405060708090a0b0c0d0e0f";

    fn rng() -> StdRng {
        StdRng::seed_from_u64(42)
    }

    #[test]
    fn text_is_hexdump_of_bytes() {
        assert_eq!(FlowId::from(BYTES).to_string(), TEXT);
    }

    #[test]
    fn subflow_is_last_four_bytes() {
        let id = FlowId::from(BYTES);
        assert_eq!(id.tree().as_bytes()[..], BYTES[..12]);
        assert_eq!(u32::from(id.subflow()), 0x0c0d_0e0f);
    }

    #[test]
    fn bytes_round_trip() {
        let id = FlowId::mint_root(Timestamp::now(), &mut rng()).mint_child(&mut rng());
        assert_eq!(FlowId::from(id.to_bytes()), id);
    }

    #[test]
    fn text_round_trip() {
        let id = FlowId::mint_root(Timestamp::now(), &mut rng()).mint_child(&mut rng());
        assert_eq!(id.to_string().parse::<FlowId>().unwrap(), id);
    }

    #[test]
    fn parses_upper_case_and_prints_lower_case() {
        let id: FlowId = TEXT.to_uppercase().parse().unwrap();
        assert_eq!(id.to_bytes(), BYTES);
        assert_eq!(id.to_string(), TEXT);
    }

    #[test]
    fn rejects_dashes() {
        let error = "00010203-0405-0607-0809-0a0b0c0d0e0f"
            .parse::<FlowId>()
            .unwrap_err();
        assert_eq!(
            error,
            IdParseError::NotHex {
                position: 8,
                found: '-'
            }
        );
    }

    #[test]
    fn rejects_hex_prefix() {
        let error = format!("0x{TEXT}").parse::<FlowId>().unwrap_err();
        assert_eq!(
            error,
            IdParseError::NotHex {
                position: 1,
                found: 'x'
            }
        );
    }

    #[test]
    fn rejects_wrong_lengths() {
        for text in ["", &TEXT[..24], &TEXT[..31], &format!("{TEXT}0")] {
            let error = text.parse::<FlowId>().unwrap_err();
            assert_eq!(
                error,
                IdParseError::WrongLength {
                    expected: IdForm::Flow,
                    found: text.len()
                },
                "input {text:?}"
            );
        }
    }

    #[test]
    fn root_has_subflow_zero() {
        assert!(FlowId::new_root().is_root());
    }

    #[test]
    fn root_carries_its_start_time() {
        let now = Timestamp::from_millisecond(1_790_000_000_000).unwrap();
        assert_eq!(FlowId::mint_root(now, &mut rng()).started_at(), now);
    }

    #[test]
    fn child_keeps_parent_tree_and_is_not_root() {
        let root = FlowId::new_root();
        let child = root.new_child();
        let grandchild = child.new_child();
        for flow in [child, grandchild] {
            assert_eq!(flow.tree(), root.tree());
            assert!(!flow.is_root());
        }
    }

    #[test]
    fn order_matches_byte_order() {
        let mut rng = rng();
        let root = FlowId::mint_root(Timestamp::now(), &mut rng);
        let mut ids: Vec<FlowId> = (0..100).map(|_| root.mint_child(&mut rng)).collect();
        ids.push(root);
        ids.sort();
        let mut bytes: Vec<[u8; 16]> = ids.iter().map(FlowId::to_bytes).collect();
        bytes.sort();
        assert_eq!(ids.iter().map(FlowId::to_bytes).collect::<Vec<_>>(), bytes);
        assert_eq!(ids[0], root);
    }
}
