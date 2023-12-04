/// Compatibility result
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompatibilityResult {
    /// is compatible
    pub is_compatible: bool,
}

/// Compatibility level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompatibilityLevel {
    #[default]
    None,
    Backward,
    Forward,
    Full,
    BackwardTransitive,
    ForwardTransitive,
    FullTransitive,
}

/// The compatibility
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Compatibility {
    /// The compatibility level
    pub compatibility: CompatibilityLevel,
}

/// The compatibility get result
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GetCompatibility {
    /// The compatibility level
    #[serde(rename = "compatibilityLevel")]
    pub compatibility_level: CompatibilityLevel,
}
