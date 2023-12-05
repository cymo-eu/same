use clap::Args;
use keyring::Entry;
use same::context::{Authentication, BasicAuthConfig, ContextRepository, LocalContextRepository};

#[derive(Args, Debug)]
pub struct AddCommand {

}

enum AuthInput {
    Basic(BasicAuthInput),
    None,
}

pub struct BasicAuthInput {
    pub username: String,
    pub password: String,
}

impl AddCommand {
    pub async fn run(&self) -> anyhow::Result<()> {
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
                    username: basic_auth.username,
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

        Ok(())
    }
}

// TODO move to context module
fn store_in_keychain(name: &String, basic_auth: &BasicAuthInput) -> anyhow::Result<String> {
    let entry_name = format!("kannika-same-{}", &name);
    let entry = Entry::new(
        entry_name.as_str(),
        basic_auth.username.as_str())?;
    entry.set_password(&basic_auth.password)?;
    Ok(entry_name)
}