use crate::context::{Context, WalkSchemaSubjectsOpts};
use crate::mapping::index::{
    Candidates, FingerprintedSchema, SchemaRegistryIndex, SchemaRegistryIndexError,
};
use crate::mapping::MappingError::OverwritingMapping;
use crate::registry::SchemaId;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub mod fingerprint;
mod index;
mod resolve;

type IndexTask = JoinHandle<IndexTaskResult>;
type IndexTaskResult = Result<SchemaRegistryIndex, SchemaRegistryIndexError>;

/// A mapping between two registries
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SchemaRegistryMapping {
    matched: BTreeMap<SchemaId, SchemaId>,
    missed: Vec<FingerprintedSchema>,
}

#[derive(Debug, thiserror::Error)]
pub enum MappingError {
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
            matched: BTreeMap::new(),
            missed: Vec::new(),
        }
    }

    /// Inserts the mapping between two schemas
    /// If there was already a mapping for the source schema, the original target schema will be returned
    /// If there was no mapping for the source schema, None will be returned
    pub fn insert_match(
        &mut self,
        source: SchemaId,
        target: SchemaId,
    ) -> Result<Option<SchemaId>, MappingError> {
        if let Some(old_mapping) = self.matched.insert(source, target) {
            if old_mapping != target {
                tracing::error!(
                    "Overwriting mapping for schema: {:?} -> {:?} (was {:?})",
                    source,
                    target,
                    old_mapping
                );
                return Err(OverwritingMapping(source, target, old_mapping));
            }
        }

        Ok(None)
    }

    pub fn insert_miss(&mut self, missed: FingerprintedSchema) {
        self.missed.push(missed);
    }

    pub fn missed(&self) -> &[FingerprintedSchema] {
        &self.missed
    }

    pub fn matched(&self) -> &BTreeMap<SchemaId, SchemaId> {
        &self.matched
    }
}

#[derive(Default)]
pub struct MapSchemasOpts {
    pub ignore_indexing_errors: bool,
}

pub async fn map_schemas(
    source: Arc<Context>,
    target: Arc<Context>,
    opts: MapSchemasOpts,
) -> Result<SchemaRegistryMapping, MappingError> {
    let index_opts = IndexOpts {
        ignore_indexing_errors: opts.ignore_indexing_errors,
    };
    let (source_index, target_index) = tokio::join!(
        spawn_index_task(source.clone(), index_opts.clone()),
        spawn_index_task(target.clone(), index_opts),
    );

    let source_index = flatten(source_index).await?;
    let target_index = flatten(target_index).await?;

    let mut mapping = SchemaRegistryMapping::new();

    // Beware, here be dragons!
    // We are iterating over the schemas of the source context,
    // and we are looking for the same schema in the target context.
    // We are using the schema fingerprint to match the schemas.
    // If the schema is not present, we will throw a warning.
    source_index.iter().for_each(|source_schema_ref| {
        let results = target_index.find_by_fingerprint(&source_schema_ref.fingerprint);

        match results {
            Candidates::PerfectMatch(match_made_in_heaven) => {
                mapping
                    .insert_match(source_schema_ref.id, match_made_in_heaven.id)
                    .unwrap();
            }
            Candidates::None => {
                tracing::warn!("Missing mapping for schema: {:?}", source_schema_ref);
                mapping.insert_miss(source_schema_ref.clone());
            }
            Candidates::Multiple(refs) => {
                let unique_ids = refs
                    .iter()
                    .map(|schema_ref| schema_ref.id)
                    .collect::<std::collections::HashSet<_>>();

                // We got multiple candidates, but they all have the same id, so we are fine
                if unique_ids.len() == 1 {
                    let first_one_is_fine = refs.first().unwrap();
                    mapping
                        .insert_match(source_schema_ref.id, first_one_is_fine.id)
                        .unwrap();

                    // We got multiple candidates, but they have different ids
                } else {
                    tracing::warn!(
                        "Multiple candidates for schema: {:?} -> {:?}",
                        source_schema_ref,
                        refs
                    );
                    mapping.insert_miss(source_schema_ref.clone());
                }
            }
        };
    });

    Ok(mapping)
}

async fn spawn_index_task(ctx: Arc<Context>, opts: IndexOpts) -> IndexTask {
    tokio::spawn(async move {
        let indexer = Indexer::new(ctx, opts);
        let idx = indexer.index().await?;
        Ok(idx)
    })
}

#[derive(Clone, Debug, Default)]
struct IndexOpts {
    ignore_indexing_errors: bool,
}

struct Indexer {
    ctx: Arc<Context>,
    opts: IndexOpts,
}

impl Indexer {
    fn new(ctx: Arc<Context>, opts: IndexOpts) -> Self {
        Self { ctx, opts }
    }

    async fn index(&self) -> IndexTaskResult {
        let mut idx = SchemaRegistryIndex::new();
        self.ctx
            .walk_schema_subjects(
                |subject| match idx.index(&subject, &self.ctx) {
                    Ok(()) => Ok(()),
                    Err(err) if self.opts.ignore_indexing_errors => {
                        tracing::warn!("Failed to index schema {:?}, ignoring: {}", subject, err);
                        Ok(())
                    }
                    Err(err) => {
                        return Err(err);
                    }
                },
                WalkSchemaSubjectsOpts::default(),
            )
            .await
            .map_err(|err| SchemaRegistryIndexError::IndexingError(err.to_string()))?;
        Ok(idx)
    }
}

async fn flatten(handle: IndexTask) -> IndexTaskResult {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => panic!("Failed to join task: {:?}", err),
    }
}
