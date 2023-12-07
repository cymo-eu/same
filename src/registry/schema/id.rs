use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;

use crate::registry::*;

/// The schema id
///
/// You can build the schema id from a string or from an `u32`
///
/// ```rust
/// use same::registry::SchemaId;
/// let id = "1".parse::<SchemaId>().expect("Should be a valid id");
/// let id2 = SchemaId::from(1);
/// assert_eq!(id, id2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, PartialOrd, Ord)]
pub struct SchemaId(u32);

impl FromStr for SchemaId {
    type Err = SchemaIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let id = s.parse::<u32>().map_err(|_| SchemaIdError(s.to_string()))?;
        Ok(Self(id))
    }
}

impl Deref for SchemaId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u32> for SchemaId {
    fn from(value: u32) -> Self {
        SchemaId(value)
    }
}

impl Display for SchemaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_build_from_str() {
        let str = "42";
        let result = SchemaId::from_str(str).unwrap();
        assert_eq!(result, SchemaId(42));
    }

    #[test]
    fn should_not_build_from_invalid_str() {
        let str = "a42";
        let result = SchemaId::from_str(str);
        assert!(result.is_err());
    }
}
