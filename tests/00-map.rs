use same::registry::{DeleteVersionOptions, ListSubjectsOptions, NewVersionOptions, RegisterSchema, SchemaRegistryClient};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn map() -> anyhow::Result<()> {
    let client = SchemaRegistryClient::new("http://localhost:8081")?;

    delete_all_subjects(&client).await?;
    register_user_schemas(&client).await?;
    register_image_event_schemas(&client).await?;

    Ok(())
}

async fn delete_all_subjects(client: &SchemaRegistryClient) -> anyhow::Result<()> {
    let subjects = client.subject().list(ListSubjectsOptions::default()).await?;
    for subject in subjects {
        for version in client.subject().versions(&subject).await? {
            let _ = client
                .subject()
                .delete_version(&subject, version, DeleteVersionOptions::default())
                .await?;
        }
    }
    Ok(())
}

async fn register_user_schemas(client: &SchemaRegistryClient) -> anyhow::Result<()> {
// Create a subject
    let subject = "user".parse()?;

    let v1 = RegisterSchema {
        schema: include_str!("assets/avro/user/v1.avsc").to_string(),
        schema_type: Some(same::registry::SchemaType::Avro),
        ..Default::default()
    };

    let v2 = RegisterSchema {
        schema: include_str!("assets/avro/user/v2.avsc").to_string(),
        schema_type: Some(same::registry::SchemaType::Avro),
        ..Default::default()
    };

    for v in &[v1, v2] {
        let _ = client
            .subject()
            .new_version(&subject, v, NewVersionOptions::default())
            .await?;
    }
    Ok(())
}

async fn register_image_event_schemas(client: &SchemaRegistryClient) -> anyhow::Result<()> {
    let subject = "image.events".parse()?;

    let v1 = RegisterSchema {
        schema: include_str!("assets/proto/image/v1.proto").to_string(),
        schema_type: Some(same::registry::SchemaType::Protobuf),
        ..Default::default()
    };

    let v2 = RegisterSchema {
        schema: include_str!("assets/proto/image/v2.proto").to_string(),
        schema_type: Some(same::registry::SchemaType::Protobuf),
        ..Default::default()
    };

    for v in &[v1, v2] {
        let _ = client
            .subject()
            .new_version(&subject, v, NewVersionOptions::default())
            .await?;
    }
    Ok(())
}