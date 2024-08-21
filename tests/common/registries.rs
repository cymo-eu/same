use anyhow::Context;
use bollard::container::Config;
use bollard::exec::{CreateExecOptions, StartExecOptions};
use bollard::models::{ContainerInspectResponse, HostConfig, PortBinding};
use bollard::Docker;

pub enum TestSchemaRegistry {
    Remote(RemoteSchemaRegistry),
    Containerized(ContainerizedSchemaRegistry),
}

pub struct RemoteSchemaRegistry {
    url: String,
}

pub struct ContainerizedSchemaRegistry {
    client: Docker,
    container_info: ContainerInspectResponse,
}

impl Drop for ContainerizedSchemaRegistry {
    fn drop(&mut self) {
        let container_id = self.container_info.id.as_ref().unwrap();
        futures::executor::block_on(async {
            self.client.stop_container(container_id, None).await.ok();
            self.client.remove_container(container_id, None).await.ok();
            self.client.remove_volume(container_id, None).await.ok();
        });
    }
}

impl RemoteSchemaRegistry {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }

    pub fn get_schema_registry_url(&self) -> String {
        self.url.to_string()
    }
}

#[allow(unused)]
impl ContainerizedSchemaRegistry {
    pub const IMAGE: &'static str = "docker.redpanda.com/redpandadata/redpanda:latest";

    pub async fn start() -> anyhow::Result<Self> {
        // What we do here is a bit tricky because we want to spawn multiple redpanda instances simultaneously.
        // To achieve that, we ask the OS to choose distinct ports randomly and then we have to start redpanda
        // with the port assigned by the OS.
        //
        // This is difficult because we have to set the `--advertise-kafka-addr` after the container and its port bindings are created.
        // Here's how it's done here:
        //
        // * We create a `exec.sh` file that will contain the command to start redpanda
        // * We create a `wait.sh` script that waits for the 'exec.sh' script to be written.
        // * We start the container and we make it wait with the 'wait.sh' script
        // * We retrieve the host port assigned by the OS for the container
        // * We fill the `exec.sh` script with the start command and the host's port
        // * The 'wait.sh' script stops waiting and run the 'exec.sh' script
        let tmpdir = tempfile::tempdir()?;

        let wait_file = tmpdir.path().join("wait.sh");
        std::fs::write(
            &wait_file,
            r#"
while [ ! -s /exec.sh ]; do
    echo "Waiting for the '/exec.sh' script"
    sleep 1;
done

/usr/bin/bash /exec.sh
        "#,
        )?;

        let exec_file = tmpdir.path().join("exec.sh");
        std::fs::write(&exec_file, "")?; // Create the file or else docker will create an empty dir when binding

        let docker = bollard::Docker::connect_with_local_defaults()?;
        let container = docker
            .create_container::<&str, _>(
                None,
                Config {
                    image: Some(Self::IMAGE),
                    host_config: Some(HostConfig {
                        // The script to run redpanda
                        binds: Some(vec![
                            format!("{}:/wait.sh:Z", wait_file.display()),
                            format!("{}:/exec.sh:Z", exec_file.display()),
                        ]),
                        // The binding with an available port chosen by the OS
                        port_bindings: Some(maplit::hashmap! {
                            "9092/tcp".to_string() => Some(vec![PortBinding {
                                host_ip: None,
                                host_port: Some("0".to_string()), // 0 == let the system decide
                            }]),
                            "8081/tcp".to_string() => Some(vec![PortBinding {
                                host_ip: None,
                                host_port: Some("0".to_string()), // 0 == let the system decide
                            }]),
                            "8082/tcp".to_string() => Some(vec![PortBinding {
                                host_ip: None,
                                host_port: Some("0".to_string()), // 0 == let the system decide
                            }]),
                        }),
                        ..HostConfig::default()
                    }),
                    // Replace redpandan's default entry point
                    entrypoint: Some(vec!["/usr/bin/bash", "/wait.sh"]),
                    ..Config::default()
                },
            )
            .await?;

        // Start the container.
        docker
            .start_container::<String>(&container.id, None)
            .await?;

        // Wait for the container to start
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Retrieve the port assigned by the OS.
        let container_info = docker.inspect_container(&container.id, None).await?;
        let kafka_port = get_kafka_port(&container_info);
        let pandaproxy_port = get_pandaproxy_port(&container_info);

        // Write the startup file now that we know the host ports
        std::fs::write(
            &exec_file,
            format!(r"rpk redpanda start --smp 1 --memory 1G --mode dev-container --advertise-kafka-addr 127.0.0.1:{kafka_port} --pandaproxy-addr 0.0.0.0:{pandaproxy_port} --advertise-pandaproxy-addr 127.0.0.1:{pandaproxy_port}"),
        )
            .context("Writing redpanda configuration")?;

        // Wait a couple of secs for the cluster to start (it avoids some err logs at the start of tests).
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        Ok(ContainerizedSchemaRegistry {
            client: docker,
            container_info,
        })
    }

    pub async fn exec_command(&self, cmd: &[&str]) -> anyhow::Result<()> {
        exec_command(&self.client, self.container_info.id.as_ref().unwrap(), cmd).await
    }

    pub async fn exec_command_detached(&self, cmd: &[&str]) -> anyhow::Result<()> {
        exec_command_detached(&self.client, self.container_info.id.as_ref().unwrap(), cmd).await
    }

    pub fn get_host_kafka_port(&self) -> u16 {
        get_kafka_port(&self.container_info)
    }

    pub fn get_host_schema_registry_port(&self) -> u16 {
        get_schema_registry_port(&self.container_info)
    }

    pub fn get_pandaproxy_port(&self) -> u16 {
        get_pandaproxy_port(&self.container_info)
    }

    pub fn get_schema_registry_url(&self) -> String {
        let host_port = self.get_host_schema_registry_port();
        format!("http://localhost:{host_port}")
    }
}

fn get_kafka_port(info: &ContainerInspectResponse) -> u16 {
    get_port(info, "9092/tcp")
}

fn get_schema_registry_port(info: &ContainerInspectResponse) -> u16 {
    get_port(info, "8081/tcp")
}

fn get_pandaproxy_port(info: &ContainerInspectResponse) -> u16 {
    get_port(info, "8082/tcp")
}

fn get_port(info: &ContainerInspectResponse, port: &str) -> u16 {
    let ports = info
        .network_settings
        .as_ref()
        .expect("Expected network settings")
        .ports
        .as_ref()
        .expect("Expected ports info");
    let host_port = ports[port]
        .as_ref()
        .expect("Expected at a matching port binding")[0]
        .host_port
        .as_ref()
        .unwrap();
    host_port.parse().unwrap()
}

async fn exec_command_detached(
    client: &Docker,
    container: &str,
    cmd: &[&str],
) -> anyhow::Result<()> {
    let exec = client
        .create_exec(
            container,
            CreateExecOptions {
                cmd: Some(cmd.to_vec()),
                ..CreateExecOptions::default()
            },
        )
        .await?;

    client
        .start_exec(
            &exec.id,
            Some(StartExecOptions {
                detach: true,
                ..Default::default()
            }),
        )
        .await?;

    Ok(())
}

async fn exec_command(client: &Docker, container: &str, cmd: &[&str]) -> anyhow::Result<()> {
    let exec = client
        .create_exec(
            container,
            CreateExecOptions {
                cmd: Some(cmd.to_vec()),
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                ..CreateExecOptions::default()
            },
        )
        .await?;
    let exec_result = client.start_exec(&exec.id, None).await?;
    match exec_result {
        bollard::exec::StartExecResults::Attached { mut output, .. } => {
            use futures::TryStreamExt;
            while let Some(bytes) = output.try_next().await? {
                dbg!(bytes);
            }
            //let _: Vec<_> = output.collect().await;
            // TODO: collect output
        }
        bollard::exec::StartExecResults::Detached => {
            //
        }
    }

    Ok(())
}
