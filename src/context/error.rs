use crate::registry;

#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    #[error("Deserialization error: {0}")]
    DeserializationError(#[source] serde_yml::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[source] serde_yml::Error),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Failed to create cache directory")]
    CacheDirCreationFailed,

    #[error("Schema registry error: {0}")]
    SchemaRegistryError(#[from] registry::SchemaRegistryClientError),

    #[error("Walk error: {0}")]
    WalkError(String),
}
