use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ContextName(pub String);

/// SameContext represents a connection and additional settings for a schema registry
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Context {
    /// The name of the context
    pub name: ContextName,

    /// The configuration for the schema registry
    pub registry: Option<SchemaRegistryConfig>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryConfig {
    pub(crate) url: String,
    pub(crate) auth: Authentication,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaslAuthentication {
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) ssl: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsAuthentication {
    pub(crate) cert: String,
    pub(crate) key: String,
    pub(crate) ca: String,
}

#[derive(Default, Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(untagged)]
pub enum Authentication {
    #[default]
    Plaintext,

    Sasl(SaslAuthentication),

    Tls(TlsAuthentication),
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