use std::sync::Arc;

use same::context::{Authentication, DownloadAllSchemaFilesOpts, EmptyDownloadProbe};
use same::mapping::{map_schemas, MapSchemasOpts};

use crate::common::TestEnv;

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logical_type_uuid() -> anyhow::Result<()> {
    common::setup_logs();

    // let env = TestEnv::new_containerized_cluster()?;
    let env = TestEnv::new_remote("http://localhost:8081")?;
    env.delete_all_subjects().await?;
    env.register_avro_schema(
        "uuid",
        include_str!("assets/avro/logicaltypes/uuid/v1.avsc"),
    )
    .await?;
    env.register_avro_schema(
        "uuid",
        include_str!("assets/avro/logicaltypes/uuid/v2.avsc"),
    )
    .await?;
    env.register_avro_schema(
        "hvac",
        include_str!("assets/avro/logicaltypes/uuid/hvac.avsc"),
    )
    .await?;

    let from = env.mk_context("from", Authentication::None)?;
    let to = env.mk_context("to", Authentication::None)?;

    from.download_all_schema_files(DownloadAllSchemaFilesOpts::<EmptyDownloadProbe>::default())
        .await?;
    to.download_all_schema_files(DownloadAllSchemaFilesOpts::<EmptyDownloadProbe>::default())
        .await?;

    let mapping = map_schemas(Arc::new(from), Arc::new(to), MapSchemasOpts::default()).await?;

    println!("{}", serde_yaml::to_string(&mapping)?);

    Ok(())
}
