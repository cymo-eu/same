use std::{fmt, io};
use std::sync::Arc;
use std::time::Duration;
use clap::Args;
use indicatif::{ProgressState, ProgressStyle};
use tokio::task::JoinHandle;
use std::io::{Write};
use dialoguer::console::Emoji;

use same::context::{Context, ContextError, ContextName, ContextRepository, DownloadAllSchemaFilesOpts, LocalContextRepository};
use same::mapping::map_schemas;

use crate::map::MapError::ContextNotFound;

#[derive(Args, Debug)]
pub struct MapCommand {
    /// The name of the context to map from
    #[arg(long)]
    from: String,
    /// The name of the context to map to
    #[arg(long)]
    to: String,
    /// Output file. Optional; if not specified, output is written to stdout
    #[arg(long, short = 'o')]
    output: Option<String>,
    /// Ignore the local cache and download all schemas again
    #[arg(long, short = 'U')]
    force_update: bool
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

        let from_ctx = Arc::new(
            repo.find_context(&from)?
                .ok_or_else(|| ContextNotFound(from))?
        );

        let to_ctx = Arc::new(
            repo.find_context(&self.to.clone().into())?
                .ok_or_else(|| ContextNotFound(to))?
        );

        if from_ctx == to_ctx {
            return Err(anyhow::anyhow!("Cannot map a context to itself"));
        }


        step(1, Emoji("🚚 ", ""),"Downloading schemas...");
        let opts = DownloadAllSchemaFilesOpts {
            ignore_cache: Some(self.force_update)
        };
        download_schemas(&from_ctx, &to_ctx, opts).await?;

        step(2, Emoji("🔎 ", ""),"Mapping schemas...");
        let mapping = map_schemas(
            from_ctx.clone(),
            to_ctx.clone(),
        ).await?;

        step(3, Emoji("🖨️", ""),"Brrrr...");
        serde_yaml::to_writer(self.output(), &mapping)?;

        step(4, Emoji("💫", ""),"Done");

        Ok(())
    }


}

impl MapCommand {
    fn output(&self) -> Box<dyn io::Write> {
        match self.output {
            Some(ref path) => Box::new(std::fs::File::create(path).unwrap()),
            None => Box::new(std::io::stdout()),
        }
    }
}

async fn download_schemas(
    from_ctx: &Arc<Context>,
    to_ctx: &Arc<Context>,
    opts: DownloadAllSchemaFilesOpts,
) -> Result<(), ContextError> {
    let progress = Arc::new(indicatif::MultiProgress::new());
    let (download_source_task, download_target_task) = tokio::join!(
            spawn_download_task(from_ctx.clone(), progress.clone(), opts.clone()),
            spawn_download_task(to_ctx.clone(), progress.clone(), opts),
        );

    flatten(download_source_task).await?;
    flatten(download_target_task).await?;
    Ok(())
}

async fn spawn_download_task(
    ctx: Arc<Context>,
    progress_bar: Arc<indicatif::MultiProgress>,
    opts: DownloadAllSchemaFilesOpts,
) ->  DownloadTask {
    tokio::spawn(async move {
        let mut progress_bar = progress_bar.add(indicatif::ProgressBar::new_spinner());
        progress_bar.enable_steady_tick(Duration::from_millis(100));
        progress_bar.tick();
        progress_bar.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg} [{wide_bar:.cyan/blue}] ({eta})")
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn fmt::Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
            .progress_chars("#>-"));
        ctx.download_all_schema_files(&mut progress_bar, opts).await?;
        progress_bar.finish_and_clear();
        Ok(())
    })
}

fn step(
    number: usize,
    emoji: Emoji,
    message: &str,
) {
    writeln!(
        io::stderr(),
        "[{}/4] {} {}",
        number,
        emoji,
        message,
    ).unwrap();
}

type DownloadTask = JoinHandle<DownloadTaskResult>;
type DownloadTaskResult = Result<(), ContextError>;

async fn flatten(
    handle: DownloadTask
) -> DownloadTaskResult {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => panic!("Failed to download schemas: {:?}", err),
    }
}
