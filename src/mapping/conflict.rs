use crate::registry::SchemaId;

// Strategy to use when multiple candidates (duplicates) are found for a schema.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, strum_macros::EnumString, strum_macros::Display,
)]
// #[strum(ascii_case_insensitive)]
#[strum(serialize_all = "kebab-case")]
pub enum ConflictResolutionStrategy {
    #[default]
    /// Duplicates are not allowed and will be considered a missing mapping.
    Strict,
    /// Pick the first schema id found.
    PickFirst,
    /// Pick the schema with the lowest id (oldest)
    PickLowestId,
    /// Pick the schema with the highest id (latest)
    PickHighestId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictResolution {
    Resolved(SchemaId),
    Unresolved,
}

impl ConflictResolutionStrategy {
    pub fn resolve(&self, candidates: impl IntoIterator<Item = SchemaId>) -> ConflictResolution {
        match self {
            ConflictResolutionStrategy::Strict => {
                let candidates: Vec<SchemaId> = candidates.into_iter().collect();
                if candidates.len() == 1 {
                    ConflictResolution::Resolved(candidates[0])
                } else {
                    ConflictResolution::Unresolved
                }
            }
            ConflictResolutionStrategy::PickFirst => {
                if let Some(first) = candidates.into_iter().next() {
                    ConflictResolution::Resolved(first)
                } else {
                    ConflictResolution::Unresolved
                }
            }
            ConflictResolutionStrategy::PickLowestId => {
                if let Some(lowest) = candidates.into_iter().min() {
                    ConflictResolution::Resolved(lowest)
                } else {
                    ConflictResolution::Unresolved
                }
            }
            ConflictResolutionStrategy::PickHighestId => {
                if let Some(highest) = candidates.into_iter().max() {
                    ConflictResolution::Resolved(highest)
                } else {
                    ConflictResolution::Unresolved
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    pub fn assert_resolved(
        strategy: ConflictResolutionStrategy,
        candidates: &[u32],
        expected: u32,
    ) {
        let candidates = candidates
            .iter()
            .map(|id| SchemaId::from(*id))
            .collect::<Vec<_>>();
        let result = strategy.resolve(candidates);
        assert_eq!(result, ConflictResolution::Resolved(expected.into()));
    }

    pub fn assert_unresolved(strategy: ConflictResolutionStrategy, candidates: &[u32]) {
        let candidates = candidates
            .iter()
            .map(|id| SchemaId::from(*id))
            .collect::<Vec<_>>();
        let result = strategy.resolve(candidates);
        assert_eq!(result, ConflictResolution::Unresolved);
    }

    mod strict {

        use super::super::ConflictResolutionStrategy;
        use super::*;

        #[test]
        fn test_single_candidate() {
            assert_resolved(ConflictResolutionStrategy::Strict, &[1], 1);
        }

        #[test]
        fn test_multiple_candidates() {
            assert_unresolved(ConflictResolutionStrategy::Strict, &[1, 2, 3]);
        }

        fn test_no_candidates() {
            assert_unresolved(ConflictResolutionStrategy::Strict, &[]);
        }
    }

    mod pick_first {
        use super::super::ConflictResolutionStrategy;
        use super::*;

        #[test]
        fn test_single_candidate() {
            assert_resolved(ConflictResolutionStrategy::PickFirst, &[1], 1);
        }

        #[test]
        fn test_multiple_candidates() {
            assert_resolved(ConflictResolutionStrategy::PickFirst, &[2, 1, 3], 2);
        }

        #[test]
        fn test_no_candidates() {
            assert_unresolved(ConflictResolutionStrategy::PickFirst, &[]);
        }
    }

    mod pick_lowest_id {
        use super::super::ConflictResolutionStrategy;
        use super::*;

        #[test]
        fn test_single_candidate() {
            assert_resolved(ConflictResolutionStrategy::PickLowestId, &[1], 1);
        }

        #[test]
        fn test_multiple_candidates() {
            assert_resolved(ConflictResolutionStrategy::PickLowestId, &[3, 1, 2], 1);
        }

        #[test]
        fn test_no_candidates() {
            assert_unresolved(ConflictResolutionStrategy::PickLowestId, &[]);
        }
    }

    mod pick_highest_id {
        use super::super::ConflictResolutionStrategy;
        use super::*;

        #[test]
        fn test_single_candidate() {
            assert_resolved(ConflictResolutionStrategy::PickHighestId, &[1], 1);
        }

        #[test]
        fn test_multiple_candidates() {
            assert_resolved(ConflictResolutionStrategy::PickHighestId, &[1, 3, 2], 3);
        }

        #[test]
        fn test_no_candidates() {
            assert_unresolved(ConflictResolutionStrategy::PickHighestId, &[]);
        }
    }

    mod parsing {
        use super::super::ConflictResolutionStrategy;

        #[test]
        fn test_default() {
            let strategy = ConflictResolutionStrategy::default();
            assert_eq!(strategy, ConflictResolutionStrategy::Strict);
        }

        #[test]
        fn test_parse_strict() {
            let strategy = "strict".parse::<ConflictResolutionStrategy>().unwrap();
            assert_eq!(strategy, ConflictResolutionStrategy::Strict);
        }

        #[test]
        fn test_parse_pick_first() {
            let strategy = "pick-first".parse::<ConflictResolutionStrategy>().unwrap();
            assert_eq!(strategy, ConflictResolutionStrategy::PickFirst);
        }

        #[test]
        fn test_parse_pick_lowest_id() {
            let strategy = "pick-lowest-id"
                .parse::<ConflictResolutionStrategy>()
                .unwrap();
            assert_eq!(strategy, ConflictResolutionStrategy::PickLowestId);
        }

        #[test]
        fn test_parse_pick_highest_id() {
            let strategy = "pick-highest-id"
                .parse::<ConflictResolutionStrategy>()
                .unwrap();
            assert_eq!(strategy, ConflictResolutionStrategy::PickHighestId);
        }
    }
}
