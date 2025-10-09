use multimap::MultiMap;

use crate::mapping::fingerprint::{FingerPrint, SubjectFingerPrintBuilder, ToFingerPrint};
use crate::mapping::resolve::ResolveSchemaReferences;
use crate::registry::{SchemaId, SchemaType, SchemaVersion, Subject, SubjectName};

/// Schema registry index that allows for fast lookup of schema references by fingerprint
pub struct SchemaRegistryIndex {
    // Index by fingerprint
    fp: MultiMap<FingerPrint, SchemaRef>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SchemaRegistryIndexError {
    #[error("Failed to calculate fingerprint for subject {0} with schema version: {1}: {2}")]
    FailedToCalculateFingerprint(SubjectName, SchemaVersion, String),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Candidates {
    Multiple(Vec<SchemaRef>),
    PerfectMatch(SchemaRef),
    None,
}

impl SchemaRegistryIndex {
    pub fn new() -> Self {
        Self {
            fp: MultiMap::new(),
        }
    }

    pub fn index(
        &mut self,
        schema_subject: &Subject,
        resolver: &impl ResolveSchemaReferences,
    ) -> Result<(), SchemaRegistryIndexError> {
        match schema_subject.schema_type {
            SchemaType::Avro => self.index_avro(schema_subject, resolver),
            SchemaType::Protobuf => Ok(()),
            SchemaType::Json => Ok(()),
        }
    }

    fn index_avro(
        &mut self,
        schema_subject: &Subject,
        resolver: &impl ResolveSchemaReferences,
    ) -> Result<(), SchemaRegistryIndexError> {
        let schema_ref: SchemaRef = to_schema_ref(schema_subject.clone(), resolver)?;

        self.insert(schema_ref);

        Ok(())
    }

    fn insert(&mut self, reference: SchemaRef) {
        self.fp
            .insert(reference.fingerprint.clone(), reference.clone());
    }

    pub fn find_by_fingerprint(&self, fingerprint: &FingerPrint) -> Candidates {
        self.fp
            .get_vec(fingerprint)
            .map(|schema_refs| schema_refs.to_owned())
            .map(|schema_refs| match schema_refs {
                mut schema_refs if schema_refs.len() == 1 => {
                    Candidates::PerfectMatch(schema_refs.pop().unwrap())
                }
                schema_refs => Candidates::Multiple(schema_refs),
            })
            .unwrap_or(Candidates::None)
    }
}

fn to_schema_ref(
    subject: Subject,
    resolver: &impl ResolveSchemaReferences,
) -> Result<SchemaRef, SchemaRegistryIndexError> {
    let fingerprint = SubjectFingerPrintBuilder::new(subject.clone())
        .resolve_references_from(resolver)
        .to_fingerprint()
        .map_err(|err| {
            SchemaRegistryIndexError::FailedToCalculateFingerprint(
                subject.subject.clone(),
                subject.version.clone(),
                err.to_string(),
            )
        })?;

    Ok(SchemaRef {
        subject: subject.subject.clone(),
        version: subject.version.clone(),
        id: subject.id.clone(),
        schema_type: subject.schema_type.clone(),
        fingerprint,
    })
}

pub struct SchemaRegistryIndexIter<'a> {
    inner: multimap::Iter<'a, FingerPrint, SchemaRef>,
}

impl<'a> Iterator for SchemaRegistryIndexIter<'a> {
    type Item = &'a SchemaRef;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, schema_ref)| schema_ref)
    }
}

impl<'a> IntoIterator for &'a SchemaRegistryIndex {
    type Item = &'a SchemaRef;
    type IntoIter = SchemaRegistryIndexIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        SchemaRegistryIndexIter {
            inner: self.fp.iter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::mapping::fingerprint::{SubjectFingerPrintBuilder, ToFingerPrint};
    use crate::mapping::index::{to_schema_ref, Candidates, SchemaRegistryIndex};
    use crate::mapping::resolve::{Resolution, ResolutionError, ResolveSchemaReferences};
    use crate::registry::{SchemaId, SchemaReference, SchemaType, SchemaVersion, Subject};

    struct MockResolver {
        mapping: Vec<(SchemaReference, Subject)>,
    }

    impl ResolveSchemaReferences for MockResolver {
        fn resolve_schema_reference(
            &self,
            reference: &SchemaReference,
        ) -> Result<Resolution, ResolutionError> {
            for (schema_ref, subject) in &self.mapping {
                if schema_ref == reference {
                    return Ok(Resolution::Resolved(schema_ref.clone(), subject.clone()));
                }
            }
            Ok(Resolution::Unresolved(reference.clone()))
        }
    }

    impl MockResolver {
        fn new() -> Self {
            Self {
                mapping: Vec::new(),
            }
        }
    }

    #[test]
    fn find_avro_schema_by_fingerprint() {
        let mut index = SchemaRegistryIndex::new();
        let schema_subject = avrocado_subject();
        let fingerprint = SubjectFingerPrintBuilder::new(schema_subject.clone())
            .to_fingerprint()
            .unwrap();

        index.index(&schema_subject, &MockResolver::new()).unwrap();

        let schema_ref = to_schema_ref(schema_subject, &MockResolver::new()).unwrap();
        let expected: Candidates = Candidates::PerfectMatch(schema_ref);

        assert_eq!(index.find_by_fingerprint(&fingerprint), expected);
    }

    #[test]
    fn find_avro_schema_with_references_by_fingerprint() {
        // Set up references
        let mut resolver = MockResolver::new();
        resolver.mapping.push((
            SchemaReference {
                name: "Product".parse().unwrap(),
                subject: "product".to_string(),
                version: "5".parse::<SchemaVersion>().unwrap(),
            },
            product_subject(),
        ));
        resolver.mapping.push((
            SchemaReference {
                name: "Customer".parse().unwrap(),
                subject: "customer".to_string(),
                version: "6".parse::<SchemaVersion>().unwrap(),
            },
            customer_subject(),
        ));

        let mut index = SchemaRegistryIndex::new();

        let schema_subject = order_subject();

        let fingerprint = SubjectFingerPrintBuilder::new(schema_subject.clone())
            .resolve_references_from(&resolver)
            .to_fingerprint()
            .unwrap();

        index.index(&schema_subject, &resolver).unwrap();

        let schema_ref = to_schema_ref(schema_subject, &resolver).unwrap();
        let expected: Candidates = Candidates::PerfectMatch(schema_ref);

        assert_eq!(index.find_by_fingerprint(&fingerprint), expected);
    }

    #[test]
    fn index_protobuf_schema_is_ignored() {
        let mut index = SchemaRegistryIndex::new();
        let schema_subject = potatobuf_subject();
        let fingerprint = SubjectFingerPrintBuilder::new(schema_subject.clone())
            .to_fingerprint()
            .unwrap();
        index.index(&schema_subject, &MockResolver::new()).unwrap();

        let results = index.find_by_fingerprint(&fingerprint);

        assert_eq!(results, Candidates::None);
    }

    #[test]
    fn index_json_schema_is_ignored() {
        let mut index = SchemaRegistryIndex::new();
        let schema_subject = jacksonfruit_subject();
        let fingerprint = SubjectFingerPrintBuilder::new(schema_subject.clone())
            .to_fingerprint()
            .unwrap();

        index.index(&schema_subject, &MockResolver::new()).unwrap();

        let results = index.find_by_fingerprint(&fingerprint);

        assert_eq!(results, Candidates::None);
    }

    fn avrocado_subject() -> Subject {
        Subject {
            subject: "avrocado".parse().unwrap(),
            version: "1".parse::<SchemaVersion>().unwrap(),
            id: "11".parse::<SchemaId>().unwrap(),
            schema_type: SchemaType::Avro,
            schema: avocado_schema().to_string(),
            references: vec![],
        }
    }

    fn potatobuf_subject() -> Subject {
        Subject {
            subject: "potatobuf".parse().unwrap(),
            version: "2".parse::<SchemaVersion>().unwrap(),
            id: "22".parse::<SchemaId>().unwrap(),
            schema_type: SchemaType::Protobuf,
            schema: potato_schema().to_string(),
            references: vec![],
        }
    }

    fn jacksonfruit_subject() -> Subject {
        Subject {
            subject: "jacksonfruit".parse().unwrap(),
            version: "3".parse::<SchemaVersion>().unwrap(),
            id: "33".parse::<SchemaId>().unwrap(),
            schema_type: SchemaType::Json,
            schema: jackfruit_schema().to_string(),
            references: vec![],
        }
    }

    fn order_subject() -> Subject {
        Subject {
            subject: "schema_reference".parse().unwrap(),
            version: "4".parse::<SchemaVersion>().unwrap(),
            id: "44".parse::<SchemaId>().unwrap(),
            schema_type: SchemaType::Avro,
            schema: order_schema().to_string(),
            references: vec![
                SchemaReference {
                    name: "Product".parse().unwrap(),
                    subject: "product".to_string(),
                    version: "5".parse::<SchemaVersion>().unwrap(),
                },
                SchemaReference {
                    name: "Customer".parse().unwrap(),
                    subject: "customer".to_string(),
                    version: "6".parse::<SchemaVersion>().unwrap(),
                },
            ],
        }
    }

    fn product_subject() -> Subject {
        Subject {
            subject: "product".parse().unwrap(),
            version: "5".parse::<SchemaVersion>().unwrap(),
            id: "55".parse::<SchemaId>().unwrap(),
            schema_type: SchemaType::Avro,
            schema: product_schema().to_string(),
            references: vec![],
        }
    }

    fn customer_subject() -> Subject {
        Subject {
            subject: "customer".parse().unwrap(),
            version: "6".parse::<SchemaVersion>().unwrap(),
            id: "66".parse::<SchemaId>().unwrap(),
            schema_type: SchemaType::Avro,
            schema: customer_schema().to_string(),
            references: vec![],
        }
    }

    fn order_schema() -> &'static str {
        r#"
            {
                "type": "record",
                "name": "Order",
                "namespace": "io.kannika",
                "fields": [
                    {
                        "name": "product",
                        "type": "io.kannika.Product"
                    },
                    {
                        "name": "customer",
                        "type": "io.kannika.Customer"
                    }
                ]
            }
            "#
    }

    fn product_schema() -> &'static str {
        r#"
            {
                "type": "record",
                "name": "Product",
                "namespace": "io.kannika",
                "fields": [
                    {
                        "name": "productName",
                        "type": "string"
                    }
                ]
            }
            "#
    }

    fn customer_schema() -> &'static str {
        r#"
            {
                "type": "record",
                "name": "Customer",
                "namespace": "io.kannika",
                "fields": [
                    {
                        "name": "customerName",
                        "type": "string"
                    }
                ]
            }
            "#
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
