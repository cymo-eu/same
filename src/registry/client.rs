struct SchemaRegistryClient {
    url: reqwest::Url,
    client: reqwest::Client,
}

enum SchemaRegistryClientError {
    InvalidUrl,
}

impl SchemaRegistryClient {

    pub fn new (url: &str) -> Result<Self, SchemaRegistryClientError> {
        let url = reqwest::Url::parse(url)
            .map_err(|_| SchemaRegistryClientError::InvalidUrl)?;
        let client = reqwest::Client::new();
        Ok(Self { url, client })
    }

}