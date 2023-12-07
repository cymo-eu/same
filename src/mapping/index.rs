use multimap::MultiMap;

use crate::mapping::fingerprint::{FingerPrint, ToFingerPrint};
use crate::registry::{SchemaId, SchemaType, SchemaVersion, Subject, SubjectName};

/// Schema registry index that allows for fast lookup of schema references by fingerprint or by schema id.
pub struct SchemaRegistryIndex {
    // Index by fingerprint
    fp: MultiMap<FingerPrint, SchemaRef>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SchemaRegistryIndexError {
    #[error("Failed to calculate fingerprint for subject {0} with schema version: {1}")]
    FailedToCalculateFingerprint(SubjectName, SchemaVersion),
    #[error("Failed to index schema: {0}")]
    IndexingError(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRef {
    pub subject: SubjectName,
    pub version: SchemaVersion,
    pub id: SchemaId,
    pub schema_type: SchemaType,
    pub fingerprint: FingerPrint,
}

impl SchemaRegistryIndex {
    pub fn new() -> Self {
        Self {
            fp: MultiMap::new(),
        }
    }

    // TODO implement proper iterator
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=&SchemaRef> + 'a {
        self.fp.iter()
            .map(|(_, schema_ref)| schema_ref)
    }

    pub fn index(
        &mut self,
        schema_subject: &Subject,
    ) -> Result<(), SchemaRegistryIndexError> {
        match schema_subject.schema_type {
            SchemaType::Avro => self.index_avro(schema_subject),
            SchemaType::Protobuf => Ok(()),
            SchemaType::Json => Ok(()),
        }
    }

    fn index_avro(
        &mut self,
        schema_subject: &Subject,
    ) -> Result<(), SchemaRegistryIndexError> {
        let schema_ref: SchemaRef = schema_subject.try_into()?;

        self.insert(schema_ref);

        Ok(())
    }


    fn insert(
        &mut self,
        reference: SchemaRef,
    ) {
        self.fp.insert(reference.fingerprint.clone(), reference.clone());
    }

    pub fn find_by_fingerprint(&self, fingerprint: &FingerPrint) -> Vec<SchemaRef> {
        self.fp.get_vec(fingerprint)
            .map(|schema_refs| schema_refs.to_owned())
            .unwrap_or_default()
    }

}

impl TryFrom<&Subject> for SchemaRef {
    type Error = SchemaRegistryIndexError;

    fn try_from(subject: &Subject) -> Result<Self, Self::Error> {
        Ok(Self {
            subject: subject.subject.clone(),
            version: subject.version.clone(),
            id: subject.id.clone(),
            schema_type: subject.schema_type.clone(),
            fingerprint: subject.to_fingerprint()
                .map_err(|_| SchemaRegistryIndexError::FailedToCalculateFingerprint(
                    subject.subject.clone(),
                    subject.version.clone()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::mapping::fingerprint::ToFingerPrint;
    use crate::mapping::index::{SchemaRef, SchemaRegistryIndex};
    use crate::registry::{SchemaId, SchemaType, SchemaVersion, Subject};


    #[test]
    fn find_avro_schema_by_fingerprint() {
        let mut index = SchemaRegistryIndex::new();
        let schema_subject = &avrocado_subject();

        index.index(schema_subject).unwrap();

        let expected: Vec<SchemaRef> = vec![schema_subject.try_into().unwrap()];

        assert_eq!(
            index.find_by_fingerprint(&schema_subject.to_fingerprint().unwrap()),
            expected);
    }

    #[test]
    fn index_protobuf_schema_is_ignored() {
        let mut index = SchemaRegistryIndex::new();
        let schema_subject = potatobuf_subject();
        index.index(&schema_subject).unwrap();

        let results = index.find_by_fingerprint(&schema_subject.to_fingerprint().unwrap());

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn index_json_schema_is_ignored() {
        let mut index = SchemaRegistryIndex::new();
        let schema_subject = jacksonfruit_subject();

        index.index(&schema_subject).unwrap();

        let results = index.find_by_fingerprint(&schema_subject.to_fingerprint().unwrap());

        assert_eq!(results.len(), 0);

    }

    fn avrocado_subject() -> Subject {
        Subject {
            subject: "avrocado".parse().unwrap(),
            version: "1".parse::<SchemaVersion>().unwrap(),
            id: "11".parse::<SchemaId>().unwrap(),
            schema_type: SchemaType::Avro,
            schema: avocado_schema().to_string(),
        }
    }

    fn potatobuf_subject() -> Subject {
        Subject {
            subject: "potatobuf".parse().unwrap(),
            version: "2".parse::<SchemaVersion>().unwrap(),
            id: "22".parse::<SchemaId>().unwrap(),
            schema_type: SchemaType::Protobuf,
            schema: potato_schema().to_string(),
        }
    }

    fn jacksonfruit_subject() -> Subject {
        Subject {
            subject: "jacksonfruit".parse().unwrap(),
            version: "3".parse::<SchemaVersion>().unwrap(),
            id: "33".parse::<SchemaId>().unwrap(),
            schema_type: SchemaType::Json,
            schema: jackfruit_schema().to_string(),
        }
    }

    fn potato_schema() -> &'static str {
        r#"
            syntax = "proto3";
            package com.example;
            message Potato {
                string name = 1;
                string color = 2;
                int32 age = 3;
            }
        "#
    }

    fn avocado_schema() -> &'static str {
        r#"
            {
                "type": "record",
                "name": "avocado",
                "namespace": "com.example",
                "fields": [
                    {
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "name": "color",
                        "type": "string"
                    },
                    {
                        "name": "age",
                        "type": "int"
                    }
                ]
            }
            "#
    }

    fn jackfruit_schema() -> &'static str {
        r#"
            {
                "type": "record",
                "name": "jackfruit",
                "namespace": "com.example",
                "fields": [
                    {
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "name": "color",
                        "type": "string"
                    },
                    {
                        "name": "age",
                        "type": "int"
                    }
                ]
            }
            "#
    }
}