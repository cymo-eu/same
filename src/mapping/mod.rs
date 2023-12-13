use std::collections::{BTreeMap};
use std::io;
use std::io::{Write};
use std::sync::Arc;
use tokio::task::JoinHandle;
use crate::context::Context;
use crate::mapping::index::{Candidates, SchemaRegistryIndex, SchemaRegistryIndexError};
use crate::mapping::MappingError::OverwritingMapping;
use crate::registry::{SchemaId};

pub mod fingerprint;
mod index;
mod resolve;

type IndexTask = JoinHandle<Result<SchemaRegistryIndex, SchemaRegistryIndexError>>;
type IndexTaskResult = Result<SchemaRegistryIndex, SchemaRegistryIndexError>;

/// A mapping between two registries
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct SchemaRegistryMapping {
    mapping: BTreeMap<SchemaId, SchemaId>,
}

#[derive(Debug, thiserror::Error)]
pub enum MappingError {
    #[error("Missing mapping for schema: {0}")]
    MissingMapping(SchemaId),

    #[error("Multiple mappings for schema: {0} -> {1:?}")]
    MultipleMappings(SchemaId, Vec<SchemaId>),

    #[error("Overwriting mapping for schema: {0} -> {1} (was {2})")]
    OverwritingMapping(SchemaId, SchemaId, SchemaId),

    #[error("Indexing error: {0}")]
    IndexingError(#[from] SchemaRegistryIndexError),
}

impl SchemaRegistryMapping {
    /// Create a new mapping
    #[must_use]
    pub fn new() -> Self {
        Self {
            mapping: BTreeMap::new()
        }
    }

    /// Inserts the mapping between two schemas
    /// If there was already a mapping for the source schema, the original target schema will be returned
    /// If there was no mapping for the source schema, None will be returned
    pub fn insert(&mut self, source: SchemaId, target: SchemaId) -> Result<Option<SchemaId>, MappingError> {
        if let Some(old_mapping)  = self.mapping.insert(source, target) {
            if old_mapping != target {
                tracing::error!("Overwriting mapping for schema: {:?} -> {:?} (was {:?})",
                        source,
                        target,
                        old_mapping);
                return Err(OverwritingMapping(source, target, old_mapping));
            }
        }

        Ok(None)
    }
}

pub async fn map_schemas(
    source: Arc<Context>,
    target: Arc<Context>,
) -> Result<SchemaRegistryMapping, MappingError> {

    let (source_index, target_index) = tokio::join!(
        spawn_index_task(source.clone()),
        spawn_index_task(target.clone()),
    );

    let source_index = flatten(source_index).await?;
    let target_index = flatten(target_index).await?;

    let mut mapping = SchemaRegistryMapping::new();

    let mut missed: usize = 0;

    // Beware, here be dragons!
    // We are iterating over the schemas of the source context,
    // and we are looking for the same schema in the target context.
    // We are using the schema fingerprint to match the schemas.
    // If the schema is not present, we will throw a warning.
    source_index.iter()
        .for_each(|source_schema_ref| {
            let results = target_index
                .find_by_fingerprint(&source_schema_ref.fingerprint);

            match results {
                Candidates::None => {
                    tracing::warn!("Missing mapping for schema: {:?}", source_schema_ref);
                    missed += 1;
                }
                Candidates::Multiple(refs) => {
                    let unique_ids = refs.iter()
                        .map(|schema_ref| schema_ref.id)
                        .collect::<std::collections::HashSet<_>>();

                    // We got multiple candidates, but they all have the same id, so we are fine
                    if unique_ids.len() == 1 {
                        let first_one_is_fine = refs.first().unwrap();
                        mapping.insert(source_schema_ref.id, first_one_is_fine.id).unwrap();

                    // We got multiple candidates, but they have different ids
                    } else {
                        tracing::warn!("Multiple candidates for schema: {:?} -> {:?}", source_schema_ref, refs);
                        missed += 1;
                    }
                }
                Candidates::PerfectMatch(match_made_in_heaven) => {
                    mapping.insert(source_schema_ref.id, match_made_in_heaven.id).unwrap();
                }
            };
        });

    if missed > 0 {
        writeln!(io::stderr(), "Missed {} schemas.", missed).unwrap();
    }

    Ok(mapping)
}

async fn index_context(
    ctx: &Context,
) -> IndexTaskResult {
    let mut idx = SchemaRegistryIndex::new();
    ctx.walk_schema_subjects(|subject| {
        idx.index(&subject, ctx)
    })
        .await
        .map_err(|err| SchemaRegistryIndexError::IndexingError(err.to_string()))?;
    Ok(idx)
}

async fn spawn_index_task(
    ctx: Arc<Context>,
) -> IndexTask {
    tokio::spawn(async move {
        let idx = index_context(&ctx).await?;
        Ok(idx)
    })
}

async fn flatten(
    handle: IndexTask
) -> IndexTaskResult {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => panic!("Failed to join task: {:?}", err),
    }
}