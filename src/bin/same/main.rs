use clap::{Args, Parser, Subcommand};
use keyring::Entry;
use same::context::{Authentication, BasicAuthConfig, ContextRepository, LocalContextRepository};


#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let opt = Opt::parse();

    match opt.command {
        Commands::Map(cmd) => {

        }
        Commands::Add(cmd) => {
            let url = dialoguer::Input::<String>::new()
                .with_prompt("Enter the url for the schema registry")
                .validate_with(|input: &String| -> Result<(), &str> {
                    if input.starts_with("http://") || input.starts_with("https://") {
                        Ok(())
                    } else {
                        Err("The url must start with http:// or https://")
                    }
                })
                .interact_text()?;

            let auth_selection = dialoguer::Select::new()
                .with_prompt("Select the authentication method")
                .items(&["Basic Auth", "None"])
                .default(0)
                .interact()?;

            let auth = match auth_selection {
                // Basic Auth
                0 => {
                    let username = dialoguer::Input::<String>::new()
                        .with_prompt("Enter the username")
                        .interact_text()?;

                    let password = dialoguer::Password::new()
                        .with_prompt("Enter the password")
                        .interact()?;

                    AuthInput::Basic(BasicAuthInput {
                        username,
                        password,
                    })
                }
                // None
                1 => {
                    AuthInput::None
                }
                _ => unreachable!(),
            };

            // Ask user for name
            let name = dialoguer::Input::<String>::new()
                .with_prompt("Enter a name for the context")
                .interact_text()?;

            // Store credentials in keyring
            let auth = match auth {
                AuthInput::Basic(basic_auth) => {
                    let entry_name = store_in_keychain(&name, &basic_auth)?;

                    Authentication::Basic (BasicAuthConfig{
                        basic_auth_entry_name: entry_name,
                    })
                }
                AuthInput::None => Authentication::None,
            };

            // Store context
            let context = same::context::Context {
                name: same::context::ContextName(name.to_owned()),
                registry: same::context::SchemaRegistryConfig {
                    url,
                    auth,
                },
            };

            let repo = LocalContextRepository::get();
            repo.set_context(context)?;

            println!("Registry {} added", &name);
        }
    }

    Ok(())
}

fn store_in_keychain(name: &String, basic_auth: &BasicAuthInput) -> anyhow::Result<String> {
    let entry_name = format!("kannika-same-{}", &name);
    let entry = Entry::new(
        entry_name.as_str(),
        basic_auth.username.as_str())?;
    entry.set_password(&basic_auth.password)?;
    Ok(entry_name)
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Opt {

    #[command(subcommand)]
    pub command: Commands,

}

#[derive(Args, Debug)]
struct MapCommand {
    #[arg(long)]
    from: String,
    #[arg(long)]
    to: String,
}

#[derive(Args, Debug)]
struct AddCommand {
}

#[derive(Subcommand, Debug)]
enum Commands {
    Map(MapCommand),
    Add(AddCommand),
}

enum AuthInput {
    Basic(BasicAuthInput),
    None,
}

pub struct BasicAuthInput {
    pub username: String,
    pub password: String,
}