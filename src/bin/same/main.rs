mod add;
mod map;

use clap::{Parser, Subcommand};
use crate::add::AddCommand;
use crate::map::MapCommand;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    // configure_text_logger();

    let opt = Opt::parse();

    let x = match opt.command {
        Commands::Map(cmd) => {
            cmd.run().await
        }
        Commands::Add(cmd) => {
            cmd.run().await
        }
    };

    match &x {
        Ok(_) => {
            tracing::info!("ok");
        }
        Err(e) => {
            tracing::error!("error: {:?}", e);
        }
    }
    x
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Opt {

    #[command(subcommand)]
    pub command: Commands,

}

#[derive(Subcommand, Debug)]
enum Commands {
    Map(MapCommand),
    Add(AddCommand),
}


/// Logger for humans. It enables some debug info
fn configure_text_logger() {
    let format = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false);
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("debug,hyper=warn"))
        .unwrap();
    tracing_subscriber::registry()
        .with(filter)
        .with(format)
        .init();
}