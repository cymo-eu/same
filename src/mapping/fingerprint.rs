use std::fmt::{Debug, Display};
use std::ops::Deref;
use avro_rs::rabin::Rabin;
use crate::registry::{Schema, SchemaId, SchemaVersion, SubjectName};
use avro_rs::Schema as AvroSchema;
use avro_rs::schema::SchemaFingerprint;

struct SubjectReference {
    subject: SubjectName,
    version: SchemaVersion,
    id: SchemaId,
}

struct FingerPrint {
    pub value: SchemaFingerprint,
}

impl FingerPrint {
    pub fn from_schema(schema: AvroSchema) -> FingerPrint {
        let value = schema.fingerprint::<Rabin>();

        FingerPrint {
            value
        }
    }
}

impl Display for FingerPrint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.deref();
        for byte in bytes {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl Deref for FingerPrint {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.value.bytes
    }
}

impl PartialEq for FingerPrint {
    fn eq(&self, other: &Self) -> bool {
        self.value.bytes == other.value.bytes
    }
}

impl Debug for FingerPrint {
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

        let fingerprint = FingerPrint::from_schema(schema);

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
            FingerPrint::from_schema(one),
            FingerPrint::from_schema(two));
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
            FingerPrint::from_schema(one),
            FingerPrint::from_schema(two));
    }
}