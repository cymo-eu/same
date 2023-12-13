use std::fmt::Display;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use indicatif::ProgressBar;
use crate::context::{Context, ContextError};
use crate::registry::{ListSubjectsOptions, SchemaReference, Subject};
use crate::registry::GetSchemaRegistryClient;

#[derive(Debug, Clone)]
pub struct DownloadAllSchemaFilesOpts {
    // Force update of all schemas
    pub ignore_cache: Option<bool>
}

impl Context {

    /// Returns the path to the cache directory for this context.
    pub fn cache_dir(&self) -> Result<PathBuf, ContextError> {
        let dir = dirs::cache_dir()
            .map(|mut path| {
                path.push("io.kannika.same");
                path.push(&self.name.deref());
                path
            })
            .ok_or(ContextError::CacheDirCreationFailed)?;

        mkdir_p(&dir)
    }

    /// Downloads all schemas from the schema registry and stores them in the cache directory.
    // TODO: Replace progress bar instrumentation with a probe
    pub async fn download_all_schema_files(
        &self,
        progress: &mut ProgressBar,
        opts: DownloadAllSchemaFilesOpts,
    ) -> Result<(), ContextError> {
        let cache_dir = self.cache_dir()?;
        let client = self.get_client()?;

        let force_update = opts.ignore_cache.unwrap_or(false);

        let subjects = client.subject()
            .list(ListSubjectsOptions::default())
            .await
            .map_err(ContextError::SchemaRegistryError)?;

        progress.set_length(subjects.len() as u64);

        for subject in subjects {
            tracing::debug!("Downloading subject: {}", subject);

            let subject_cache_dir = mkdir_p(&cache_dir.join(subject.deref()))?;

            let versions = client.subject()
                .versions(&subject)
                .await
                .map_err(ContextError::SchemaRegistryError)?;

            for version in versions {
                tracing::debug!("Downloading subject  {} version {}", subject, version);

                let message = format!(
                    "{:<8} / {:<12} / {:<3}",
                    if self.name.len() > 8 {
                        format!("{:.5}...", substr(self.name.deref(), 0, 5))
                    } else {
                        format!("{:.8}", self.name)
                    },
                    if subject.len() > 12 {
                        format!("{:.9}...", substr(subject.deref(), 0, 9))
                    } else {
                        format!("{:.12}", subject)
                    },
                    format!("{:.5}", version));

                progress.set_message(message);

                // Check if we already have this schema cached
                let schema_file = subject_cache_dir.join(version.to_string());

                if !force_update && schema_file.exists() {
                    tracing::debug!("Schema already cached at {}", schema_file.display());
                    continue;
                }

                let schema = client.subject().version(&subject, version).await?;

                if let Some(schema) = schema {
                    let schema_file = subject_cache_dir.join(version.to_string());

                    tracing::trace!("Writing schema to {}", schema_file.display());

                    let file = std::fs::File::create(schema_file)
                        .map_err(ContextError::IoError)?;

                    serde_yaml::to_writer(file, &schema)
                        .map_err(ContextError::SerializationError)?;
                } else {
                    tracing::debug!("No schema found for subject {} version {}", subject, version)
                }
            }

            progress.inc(1);
        }

        Ok(())
    }

    /// Walks all schema subjects in the cache directory and calls the given function for each subject.
    pub async fn walk_schema_subjects<T, E: Display>(
        &self,
        mut f: impl FnMut(Subject) -> Result<T, E>,
    ) -> Result<(), ContextError> {
        let cache_dir = self.cache_dir()?;

        for entry in std::fs::read_dir(cache_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                for entry in std::fs::read_dir(path)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_file() {
                        let file = std::fs::File::open(&path)?;

                        let subject: Subject = serde_yaml::from_reader(file)
                            .map_err(ContextError::DeserializationError)?;

                        f(subject)
                            .map_err(|e| ContextError::WalkError(e.to_string()))?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns the number of schema subjects in the cache directory.
    pub async fn count_subjects(&self) -> Result<usize, ContextError> {
        let mut count = 0;

        self.walk_schema_subjects::<(), ContextError>(|_| {
            count += 1;
            Ok(())
        }).await?;

        Ok(count)
    }

    pub fn get_subject(&self, reference: &SchemaReference) -> Result<Option<Subject>, ContextError> {
        let cache_dir = self.cache_dir()?;

        let subject_cache_dir = cache_dir.join(reference.subject.deref());

        let schema_file = subject_cache_dir.join(reference.version.to_string());

        if schema_file.exists() {
            let file = std::fs::File::open(schema_file)?;

            let subject: Subject = serde_yaml::from_reader(file)
                .map_err(ContextError::DeserializationError)?;

            Ok(Some(subject))
        } else {
            Ok(None)
        }
    }
}

fn mkdir_p<P: AsRef<Path>>(path: P) -> Result<PathBuf, ContextError> {
    let path = path.as_ref();

    std::fs::create_dir_all(path)
        .map_err(ContextError::IoError)?;

    Ok(path.to_path_buf())
}

fn substr(s: &str, start: usize, end: usize) -> String {
    match s.char_indices().nth(start) {
        Some((start_idx, _)) => {
            match s.char_indices().nth(end) {
                Some((end_idx, _)) => s[start_idx..end_idx].to_string(),
                None => s[start_idx..].to_string(),
            }
        },
        None => String::new(),
    }
}
