use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;
use crate::registry::SubjectNameError;

/// A subject name
///
/// You can build the schema id from a string
///
/// ```rust
/// # use same::registry::SubjectName;
/// let a_topic_value = "a-topic-value".parse::<SubjectName>().expect("Should be a valid name");
/// let a_topic_key = "a-topic-key".parse::<SubjectName>().expect("Should be a valid name");
/// ```
///
/// Note that name could not contains control characters
///
/// ```rust
/// # use same::registry::SubjectName;
/// let result = "\n".parse::<SubjectName>(); // 🚨 Error
/// assert!(result.is_err());
/// ```
///
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SubjectName(String);

impl AsRef<str> for SubjectName {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl Deref for SubjectName {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromStr for SubjectName {
    type Err = SubjectNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(Self::Err::EmptyName);
        }
        if s.chars().any(char::is_control) {
            return Err(Self::Err::InvalidChar(s.to_string()));
        }
        Ok(Self(s.to_string()))
    }
}

impl Display for SubjectName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::registry::SubjectNameError;

    use super::*;

    #[test]
    fn should_parse_subject_name() {
        let name = "sensor-values";
        let result = name.parse::<SubjectName>();
        let _expected: Result<SubjectName, SubjectNameError> = Ok(SubjectName("sensor-values".to_string()));
        assert!(matches!(result, _expected));
    }

    #[test]
    fn should_not_parse_empty_subject_name() {
        let name = "";
        let result = name.parse::<SubjectName>();
        let _expected: Result<SubjectName, SubjectNameError> = Err(SubjectNameError::EmptyName);
        assert!(matches!(result, _expected));
    }

    #[test]
    fn should_not_parse_bad_subject_name() {
        let name = "\nasd";
        let result = name.parse::<SubjectName>();
        let _expected: Result<SubjectName, SubjectNameError> = Err(SubjectNameError::InvalidChar(name.to_string()));
        assert!(matches!(result, _expected));
    }
}
