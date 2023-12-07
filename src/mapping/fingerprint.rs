use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::Deref;

use avro_rs::rabin::Rabin;
use avro_rs::Schema as AvroSchema;
use avro_rs::schema::SchemaFingerprint;

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
    InvalidAvroSchema(#[from] avro_rs::Error),

}

pub trait ToFingerPrint {
    fn to_fingerprint(&self) -> Result<FingerPrint, FingerPrintError>;
}

impl ToFingerPrint for Subject {
    fn to_fingerprint(&self) -> Result<FingerPrint, FingerPrintError> {
        match self.schema_type {
            SchemaType::Avro => {
                let schema = AvroSchema::parse_str(self.schema.as_str())
                    .map_err(|e| FingerPrintError::InvalidAvroSchema(e))?;
                let fingerprint = AvroFingerPrint::from_schema(schema);
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

pub struct AvroFingerPrint {
    pub value: SchemaFingerprint,
}

impl AvroFingerPrint {
    pub fn from_schema(schema: AvroSchema) -> AvroFingerPrint {
        let value = schema.fingerprint::<Rabin>();

        AvroFingerPrint {
            value
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

impl Clone for AvroFingerPrint {
    fn clone(&self) -> Self {
        AvroFingerPrint {
            value: SchemaFingerprint {
                bytes: self.value.bytes.clone()
            }
        }
    }

}

impl Deref for AvroFingerPrint {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.value.bytes
    }
}

impl PartialEq for AvroFingerPrint {
    fn eq(&self, other: &Self) -> bool {
        self.value.bytes == other.value.bytes
    }
}

impl Eq for AvroFingerPrint {

}

impl Hash for AvroFingerPrint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.bytes.hash(state);
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
    use super::*;

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

        let fingerprint = AvroFingerPrint::from_schema(schema);

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
            AvroFingerPrint::from_schema(one),
            AvroFingerPrint::from_schema(two));
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
            AvroFingerPrint::from_schema(one),
            AvroFingerPrint::from_schema(two));
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
            AvroFingerPrint::from_schema(one),
            AvroFingerPrint::from_schema(two));
    }
}