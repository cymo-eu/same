use std::ops::Deref;
use std::path::{Path, PathBuf};
use indicatif::ProgressBar;
use crate::context::{Context, ContextError};
use crate::registry::ListSubjectsOptions;
use crate::registry::GetSchemaRegistryClient;

impl Context {

    fn cache_dir(&self) -> Result<PathBuf, ContextError> {
        let dir = dirs::cache_dir()
            .map(|mut path| {
                path.push("io.kannika.same");
                path.push(&self.name.deref());

                // // Generate a random string to avoid collisions
                // let random_string = rand::thread_rng()
                //     .sample_iter(&rand::distributions::Alphanumeric)
                //     .take(10)
                //     .map(char::from)
                //     .collect::<String>();
                // path.push(random_string);

                path
            })
            .ok_or(ContextError::CacheDirCreationFailed)?;

        mkdir_p(&dir)
    }

    pub async fn download_all_schema_files(
        &self,
        progress: &mut ProgressBar,
    ) -> anyhow::Result<()> {

        let cache_dir = self.cache_dir()?;

        let client = self.get_client()?;

        let subjects = client.subject()
            .list(ListSubjectsOptions::default()).await?;

        progress.set_length(subjects.len() as u64);

        for subject in subjects {
            let subject_cache_dir = mkdir_p(&cache_dir.join(subject.deref()))?;

            let versions = client.subject().versions(&subject).await?;

            for version in versions {
                progress.set_message(format!("Downloading {} / {} / {}", self.name, subject, version));
                let schema = client.subject().version(&subject, version).await?;
                if let Some(schema) = schema {
                    let schema_file = subject_cache_dir.join(version.to_string());
                    tracing::trace!("Writing schema to {}", schema_file.display());
                    serde_yaml::to_writer(std::fs::File::create(schema_file)?, &schema)?;
                }
            }

            progress.inc(1);
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