mod name;

use crate::registry::{SchemaId, SchemaReference, SchemaType, SchemaVersion};

pub use self::name::*;

/// A schema
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subject {
    /// Name of the subject
    pub subject: SubjectName,
    /// Id of the schema
    pub id: SchemaId,
    /// Version of the schema
    pub version: SchemaVersion,
    /// The schema type
    #[serde(rename="schemaType", default)]
    pub schema_type: SchemaType,
    /// The schema
    pub schema: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub references: Vec<SchemaReference>,
}

/// Register a schema
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterSchema {
    /// The schema string
    pub schema: String,
    /// The schema type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_type: Option<SchemaType>,
    /// The schema references
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<SchemaReference>,
}

/// Registered schema result
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RegisteredSchema {
    /// The schema id
    pub id: SchemaId,
}

#[cfg(test)]
mod tests {
    use crate::registry::{SchemaId, SchemaVersion, Subject, SubjectName};

    #[test]
    fn parse_protobuf_subject() {
        let subject: Subject = serde_json::from_str(
            r#"{
                "subject": "potatobuf",
                "version": 1,
                "id": 2,
                "schema": "schema",
                "schemaType": "PROTOBUF"
            }"#,
        ).unwrap();

        assert_eq!(subject, Subject {
            subject: "potatobuf".parse::<SubjectName>().unwrap(),
            version: "1".parse::<SchemaVersion>().unwrap(),
            id: "2".parse::<SchemaId>().unwrap(),
            schema_type: crate::registry::SchemaType::Protobuf,
            schema: "schema".to_owned(),
            references: vec![],
        });
    }

}