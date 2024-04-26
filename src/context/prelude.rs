use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ContextName(pub String);

/// SameContext represents a connection and additional settings for a schema registry
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Context {
    /// The name of the context
    pub name: ContextName,

    /// The configuration for the schema registry
    pub registry: SchemaRegistryConfig,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryConfig {
    pub url: String,
    pub auth: Authentication,
}

#[derive(Default, Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Authentication {
    #[default]
    None,

    Keychain(KeychainConfig),

    BasicAuth {
        username: String,
        password: String,
    },
}

#[derive(Default, Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeychainConfig {
    pub username: String,
    pub basic_auth_entry_name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, thiserror::Error)]
pub enum ContextNameError {
    Empty,
}

impl FromStr for ContextName {
    type Err = ContextNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(Self::Err::Empty);
        }

        Ok(Self(s.to_owned()))
    }
}

impl From<String> for ContextName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ContextName {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl Deref for ContextName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for ContextName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for ContextNameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContextNameError::Empty => write!(f, "Context name cannot be empty"),
        }
    }
}

impl ContextName {
    pub fn new(s: &str) -> Result<Self, ContextNameError> {
        Self::from_str(s)
    }
}