use std::error::Error;
use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;
use clap::Args;
use indicatif::{ProgressState, ProgressStyle};
use tokio::task::JoinHandle;
use tracing::info;

use same::context::{Context, ContextName, ContextRepository, LocalContextRepository};

use crate::map::MapError::ContextNotFound;

#[derive(Args, Debug)]
pub struct MapCommand {
    #[arg(long)]
    from: String,
    #[arg(long)]
    to: String,
}

#[derive(Debug, thiserror::Error)]
enum MapError {
    #[error("Context not found: {0}")]
    ContextNotFound(ContextName),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

}

impl MapCommand {
    pub async fn run(&self) -> anyhow::Result<()> {
        let repo = LocalContextRepository::get();

        let from: ContextName = self.from.clone().into();
        let to: ContextName = self.to.clone().into();

        let from_ctx = repo.find_context(&from)?
            .ok_or_else(|| ContextNotFound(from))?;

        let to_ctx = repo.find_context(&self.to.clone().into())?
            .ok_or_else(|| ContextNotFound(to))?;

        let progress_bar = Arc::new(indicatif::MultiProgress::new());

        info!("Downloading schemas...");
        //spawn task
        let from_task = spawn_download_task(Arc::new(from_ctx), progress_bar.clone()).await;
        let to_task = spawn_download_task(Arc::new(to_ctx), progress_bar).await;

        //wait for tasks to finish
        let _ = tokio::join!(from_task, to_task);

        info!("Generating schema mapping file...");



        println!("Done");

        Ok(())
    }

}

async fn spawn_download_task(
    ctx: Arc<Context>,
    progress_bar: Arc<indicatif::MultiProgress>
) -> JoinHandle<Result<(), Box<dyn Error + Send>>> {
    tokio::spawn(async move {
        let mut progress_bar = progress_bar.add(indicatif::ProgressBar::new_spinner());
        progress_bar.enable_steady_tick(Duration::from_millis(100));
        progress_bar.tick();
        progress_bar.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg} [{wide_bar:.cyan/blue}] ({eta})")
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
            .progress_chars("#>-"));
        ctx.download_all_schema_files(&mut progress_bar).await?;
        progress_bar.finish_and_clear();
        Ok(())
    })
}

