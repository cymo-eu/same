use crate::common::TestEnv;
use same::context::{Authentication, DownloadAllSchemaFilesOpts, EmptyDownloadProbe};
use same::mapping::{map_schemas, MapSchemasOpts};
use std::sync::Arc;

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_map_schemas() -> anyhow::Result<()> {
    common::setup_logs();

    let env = TestEnv::new_remote("http://localhost:8081")?;

    env.delete_all_subjects().await?;
    let _ = env
        .register_avro_schema("user", include_str!("assets/avro/user/v1.avsc"))
        .await?;
    let _ = env
        .register_avro_schema("user", include_str!("assets/avro/user/v2.avsc"))
        .await?;

    let _ = env
        .register_protobuf_schema("image", include_str!("assets/proto/image/v1.proto"))
        .await?;
    let _ = env
        .register_protobuf_schema("image", include_str!("assets/proto/image/v2.proto"))
        .await?;

    let from = env.mk_context("from", Authentication::None)?;
    let to = env.mk_context("to", Authentication::None)?;

    from.download_all_schema_files(DownloadAllSchemaFilesOpts::<EmptyDownloadProbe>::default())
        .await?;
    to.download_all_schema_files(DownloadAllSchemaFilesOpts::<EmptyDownloadProbe>::default())
        .await?;

    let mapping = map_schemas(Arc::new(from), Arc::new(to), MapSchemasOpts::default()).await?;

    assert!(mapping.missed().is_empty(), "expected no missed schemas");
    assert_eq!(mapping.matched().len(), 4, "expected 4 matched schemas");

    Ok(())
}
