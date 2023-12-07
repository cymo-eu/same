use std::fmt::Display;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use indicatif::ProgressBar;
use crate::context::{Context, ContextError};
use crate::registry::{ListSubjectsOptions, Subject};
use crate::registry::GetSchemaRegistryClient;

impl Context {

    pub async fn cache_dir(&self) -> Result<PathBuf, ContextError> {
        let dir = dirs::cache_dir()
            .map(|mut path| {
                path.push("io.kannika.same");
                path.push(&self.name.deref());
                path
            })
            .ok_or(ContextError::CacheDirCreationFailed)?;

        mkdir_p(&dir)
    }

    pub async fn download_all_schema_files(
        &self,
        progress: &mut ProgressBar,
    ) -> Result<(), ContextError> {

        let cache_dir = self.cache_dir().await?;

        let client = self.get_client()?;

        let subjects = client.subject()
            .list(ListSubjectsOptions::default())
            .await
            .map_err(ContextError::SchemaRegistryError)?;

        progress.set_length(subjects.len() as u64);

        for subject in subjects {
            tracing::debug!("Downloading subject: {:?}", subject);

            let subject_cache_dir = mkdir_p(&cache_dir.join(subject.deref()))?;

            let versions = client.subject()
                .versions(&subject)
                .await
                .map_err(ContextError::SchemaRegistryError)?;

            for version in versions {
                tracing::debug!("Downloading subject  {:?} version {:?}", subject, version);

                progress.set_message(format!("Downloading {} / {} / {}", self.name, subject, version));

                let schema = client.subject().version(&subject, version).await?;

                if let Some(schema) = schema {
                    let schema_file = subject_cache_dir.join(version.to_string());

                    tracing::trace!("Writing schema to {}", schema_file.display());

                    let file = std::fs::File::create(schema_file)
                        .map_err(ContextError::IoError)?;

                    serde_yaml::to_writer(file, &schema)
                        .map_err(ContextError::SerializationError)?;
                } else {
                    tracing::debug!("No schema found for subject {:?} version {:?}", subject, version)
                }
            }

            progress.inc(1);
        }

        Ok(())
    }

    pub async fn walk_schema_subjects<T,E: Display>(
        &self,
        mut f: impl FnMut(Subject) -> Result<T,E> ,
    ) -> anyhow::Result<()> {
        let cache_dir = self.cache_dir().await?;

        for entry in std::fs::read_dir(cache_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                for entry in std::fs::read_dir(path)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_file() {
                        let subject: Subject = serde_yaml::from_reader(std::fs::File::open(&path)?)?;
                        f(subject)
                            .map_err(|e| anyhow::anyhow!("Error while processing subject: {}", e))?;
                    }
                }
            }
        }

        Ok(())
    }

}

fn mkdir_p<P: AsRef<Path>>(path: P) -> Result<PathBuf, ContextError> {
    let path = path.as_ref();

    std::fs::create_dir_all(path)
        .map_err(ContextError::IoError)?;

    Ok(path.to_path_buf())
}