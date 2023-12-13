use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::Deref;

use apache_avro::rabin::Rabin;
use apache_avro::Schema as AvroSchema;

use crate::mapping::resolve::{Resolution, ResolveSchemaReferences};
use crate::registry::{SchemaType, Subject};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FingerPrint {
    Avro(AvroFingerPrint),
    Protobuf,
    Json
}

#[derive(Debug, thiserror::Error)]
pub enum FingerPrintError {

    #[error(transparent)]
    InvalidAvroSchema(#[from] apache_avro::Error),

}

pub trait ToFingerPrint {
    fn to_fingerprint(&self) -> Result<FingerPrint, FingerPrintError>;
}

pub struct SubjectFingerPrintBuilder {
    pub subject: Subject,
    pub referenced_schemas: Vec<String>,
}

impl SubjectFingerPrintBuilder {
    pub fn new(subject: Subject) -> SubjectFingerPrintBuilder {
        SubjectFingerPrintBuilder {
            subject,
            referenced_schemas: Vec::new(),
        }
    }

    pub fn resolve_references_from(
        &mut self,
        resolver: &impl ResolveSchemaReferences
    ) -> &SubjectFingerPrintBuilder {
        let mut resolved = Vec::new();

        for reference in self.subject.references.iter() {
            match resolver.resolve_schema_reference(reference) {
                Ok(Resolution::Resolved(_schema_ref, resolved_schema)) => {
                    resolved.push(resolved_schema.schema);
                }
                Ok(Resolution::Unresolved(schema_ref)) => {
                   tracing::warn!("Unresolved schema reference: {:?}", schema_ref)
                }
                Err(err) => {
                    tracing::error!("Error resolving schema reference: {:?}", err)
                }
            }
        }
        self.referenced_schemas = resolved;
        self
    }
}

impl ToFingerPrint for SubjectFingerPrintBuilder {
    fn to_fingerprint(&self) -> Result<FingerPrint, FingerPrintError> {
        match self.subject.schema_type {
            SchemaType::Avro => {
                let mut schemas = Vec::<&str>::new();

                // Add the subject schema, MUST be first of the list
                schemas.push(self.subject.schema.as_str());

                for schema in self.referenced_schemas.iter() {
                    schemas.push(schema.as_str());
                }

                let input = &schemas[..];

                let schema = AvroSchema::parse_list(input)
                    .map_err(|e| FingerPrintError::InvalidAvroSchema(e))?;

                // Get the first schema in the list
                let first = schema.first().unwrap();

                let fingerprint = AvroFingerPrint::from_schema(&first);

                Ok(FingerPrint::Avro(fingerprint))
            },
            SchemaType::Json => {
                Ok(FingerPrint::Json)
            },
            SchemaType::Protobuf => {
                Ok(FingerPrint::Protobuf)
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AvroFingerPrint {
    pub bytes: Vec<u8>,
}


impl AvroFingerPrint {
    pub fn from_schema(schema: &AvroSchema) -> AvroFingerPrint {
        let fingerprint = schema.fingerprint::<Rabin>();

        AvroFingerPrint {
            bytes: fingerprint.bytes
        }
    }
}

impl Display for AvroFingerPrint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.deref();
        for byte in bytes {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl Deref for AvroFingerPrint {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl Debug for AvroFingerPrint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.deref();
        for byte in bytes {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::AvroSchema;
    use crate::mapping::fingerprint::AvroFingerPrint;

    #[test]
    fn display_should_print_fingerprint() {
        let schema = r#"
        {
            "type": "record",
            "name": "test",
            "namespace": "com.example",
            "fields": [
                {
                    "name": "a",
                    "type": "long"
                }
            ]
        }
        "#;

        let schema = AvroSchema::parse_str(schema).unwrap();

        let fingerprint = AvroFingerPrint::from_schema(&schema);

        assert_eq!(format!("{}", fingerprint), "6c286d2ee6d243cd");
    }

    #[test]
    fn same_schemas_should_have_same_fingerprint() {
        let one = AvroSchema::parse_str(r#"
        {
            "type": "record",
            "name": "test",
            "namespace": "com.example",
            "fields": [
                {
                    "name": "a",
                    "type": "long"
                }
            ]
        }
        "#).unwrap();

        let two = AvroSchema::parse_str(r#"
        {
            "namespace": "com.example",
            "type": "record",
            "name": "test",
            "fields": [
                {
                    "type": "long",
                    "name": "a"
                }
            ]
        }
        "#).unwrap();

        assert_eq!(
            AvroFingerPrint::from_schema(&one),
            AvroFingerPrint::from_schema(&two));
    }

    #[test]
    fn different_schemas_should_have_different_fingerprint() {
        let one = AvroSchema::parse_str(r#"
        {
            "type": "record",
            "name": "test",
            "namespace": "com.example",
            "fields": [
                {
                    "name": "a",
                    "type": "long"
                }
            ]
        }
        "#).unwrap();

        let two = AvroSchema::parse_str(r#"
        {
            "namespace": "com.example",
            "type": "record",
            "name": "test",
            "fields": [
                {
                    "type": "string",
                    "name": "a"
                }
            ]
        }
        "#).unwrap();

        assert_ne!(
            AvroFingerPrint::from_schema(&one),
            AvroFingerPrint::from_schema(&two));
    }

    #[test]
    fn docs_should_be_ignored() {
        let one = AvroSchema::parse_str(r#"
        {
            "type": "record",
            "docs": "Experience is that marvelous thing that enables you recognize a mistake when you make it again.",
            "name": "test",
            "namespace": "com.example",
            "fields": [
                {
                    "name": "a",
                    "type": "long"
                }
            ]
        }
        "#).unwrap();

        let two = AvroSchema::parse_str(r#"
        {
            "namespace": "com.example",
            "type": "record",
            "name": "test",
            "fields": [
                {
                    "type": "long",
                    "name": "a",
                    "docs": "Trying to establish voice contact ... please yell into keyboard."
                }
            ]
        }
        "#).unwrap();

        assert_eq!(
            AvroFingerPrint::from_schema(&one),
            AvroFingerPrint::from_schema(&two));
    }
}