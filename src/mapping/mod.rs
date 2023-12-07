use std::collections::{BTreeMap};
use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;
use indicatif::{ProgressState, ProgressStyle};
use tokio::task::JoinHandle;
use crate::context::Context;
use crate::mapping::index::{SchemaRegistryIndex, SchemaRegistryIndexError};
use crate::registry::SchemaId;

pub mod fingerprint;
mod index;

type IndexTask = JoinHandle<Result<SchemaRegistryIndex, SchemaRegistryIndexError>>;
type IndexTaskResult = Result<SchemaRegistryIndex, SchemaRegistryIndexError>;

/// A mapping between two registries
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct SchemaRegistryMapping {
    mapping: BTreeMap<SchemaId, SchemaId>,
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
    pub fn insert(&mut self, source: SchemaId, target: SchemaId) -> Option<SchemaId> {
        self.mapping.insert(source, target)
    }
}

pub async fn map_schemas(
    source: Arc<Context>,
    target: Arc<Context>,
) -> anyhow::Result<SchemaRegistryMapping> {
    let progress = Arc::new(indicatif::MultiProgress::new());

    let (source_index, target_index) = tokio::join!(
        spawn_index_task(source.clone(), progress.clone()),
        spawn_index_task(target.clone(), progress.clone()),
    );

    let source_index = flatten(source_index).await?;
    let target_index = flatten(target_index).await?;

    let mut mapping = SchemaRegistryMapping::new();

    // Beware, here be dragons!
    // We are iterating over the schemas of the source context,
    // and we are looking for the same schema in the target context.
    // We are using the schema fingerprint to find the schema in the target context.
    // If the schema is not present, we will throw a warning.
    source_index.iter()
        .for_each(|source_schema_ref| {
            let results = target_index
                .find_by_fingerprint(&source_schema_ref.fingerprint);

            match results {
                _no_candidates if results.len() == 0 => {
                    tracing::warn!("Missing mapping for schema: {:?}", source_schema_ref);
                },
                _multiple_candidates if results.len() > 1 => {
                    tracing::warn!("Multiple candidates for schema: {:?}", source_schema_ref);
                },
                match_made_in_heaven if results.len() == 1 => {
                    let target_schema = match_made_in_heaven
                        .iter()
                        .next()
                        .unwrap();
                    if let Some(old_mapping) = mapping.mapping.insert(
                        source_schema_ref.id,
                        target_schema.id) {
                        tracing::warn!("Overwriting mapping for schema: {:?} -> {:?} (was {:?})", source_schema_ref, target_schema, old_mapping);
                    }
                },
                _ => unreachable!("If len() is broken and returns negative lengths, we should hide in a corner and cry"),
            };

        });
    Ok(mapping)
}

async fn index_context(
    ctx: &Context,
) -> IndexTaskResult {
    let mut idx = SchemaRegistryIndex::new();
    ctx.walk_schema_subjects(|subject| {
        idx.index( &subject)
    })
        .await
        .map_err(|err| SchemaRegistryIndexError::IndexingError(err.to_string()))?;
    Ok(idx)
}

async fn spawn_index_task(
    ctx: Arc<Context>,
    progress_bar: Arc<indicatif::MultiProgress>,
) -> IndexTask {
    tokio::spawn(async move {
        let progress_bar = progress_bar.add(indicatif::ProgressBar::new_spinner());
        progress_bar.enable_steady_tick(Duration::from_millis(100));
        progress_bar.tick();
        progress_bar.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg} [{wide_bar:.cyan/blue}] ({eta})")
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
            .progress_chars("#>-"));
        let idx = index_context(&ctx).await?;
        progress_bar.finish_and_clear();
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
