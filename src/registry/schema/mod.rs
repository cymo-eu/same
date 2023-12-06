mod id;
mod reference;
mod version;

use std::fmt::Display;

pub use self::id::*;
pub use self::reference::*;
pub use self::version::*;

/// A Schema type
#[derive(Debug, Clone, Copy, PartialEq, Default, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[derive(strum_macros::EnumString, strum_macros::Display)]
#[strum(ascii_case_insensitive)]
pub enum SchemaType {
    /// Avro
    #[default]
    Avro,
    /// Protobuf
    Protobuf,
    /// JSON
    Json,
}


/// A Schema payload
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Schema {
    /// The schema as string
    pub schema: String,
}


#[cfg(test)]
mod tests {

    #[test]
    fn test_parse_avro_schema_type() {
        let schema_type = "avro";
        let schema_type = schema_type.parse::<super::SchemaType>().unwrap();
        assert_eq!(schema_type, super::SchemaType::Avro);
    }

    // parse protobuf schema type
    #[test]
    fn test_parse_protobuf_schema_type() {
        let schema_type = "protobuf";
        let schema_type = schema_type.parse::<super::SchemaType>().unwrap();
        assert_eq!(schema_type, super::SchemaType::Protobuf);
    }

    // parse json schema type
    #[test]
    fn test_parse_json_schema_type() {
        let schema_type = "json";
        let schema_type = schema_type.parse::<super::SchemaType>().unwrap();
        assert_eq!(schema_type, super::SchemaType::Json);
    }

}