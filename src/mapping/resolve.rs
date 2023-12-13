use crate::context::{Context, ContextError};
use crate::registry::{SchemaReference, Subject};

pub trait ResolveSchemaReferences {
    fn resolve_schema_reference(&self, reference: &SchemaReference) -> Result<Resolution, ResolutionError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ResolutionError {

    #[error(transparent)]
    ContextError(#[from] ContextError),

}

/// The result of resolving a schema reference.
/// ‘Resolved’ means that the schema was found in the context.
/// ‘Unresolved’ means that the schema was not found in the context.
pub enum Resolution {
    /// The schema was resolved.
    Resolved(SchemaReference, Subject),

    /// The schema was not resolved
    Unresolved(SchemaReference),
}

impl ResolveSchemaReferences for Context {

    fn resolve_schema_reference(
        &self,
        reference: &SchemaReference
    ) -> Result<Resolution, ResolutionError> {
        match self.get_subject(reference) {
            Ok(Some(schema)) => Ok(Resolution::Resolved(reference.clone(), schema)),
            Ok(None) => Ok(Resolution::Unresolved(reference.clone())),
            Err(err) => Err(err.into()),
        }
    }
}