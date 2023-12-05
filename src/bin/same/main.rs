mod add;
mod map;

use clap::{Parser, Subcommand};
use crate::add::AddCommand;
use crate::map::MapCommand;

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    tracing_subscriber::fmt::init();

    let opt = Opt::parse();

    match opt.command {
        Commands::Map(cmd) => {
            cmd.run().await?;
        }
        Commands::Add(cmd) => {
            cmd.run().await?;
        }
    }

    Ok(())
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

