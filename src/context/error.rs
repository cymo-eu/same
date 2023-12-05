
#[derive(Debug, thiserror::Error)]
pub enum ContextError {

    #[error("Deserialization error: {0}")]
    DeserializationError(#[source] serde_yaml::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[source] serde_yaml::Error),

    #[error("I/O error: {0}")]
    IoError(#[source] std::io::Error),

    #[error("Failed to create cache directory")]
    CacheDirCreationFailed,
}