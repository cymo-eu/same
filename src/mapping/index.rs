use multimap::MultiMap;

use crate::AvroSchema;
use crate::context::ContextName;
use crate::mapping::fingerprint::{FingerPrint, ToFingerPrint};
use crate::registry::{SchemaId, SchemaType, SchemaVersion, Subject, SubjectName};

struct SchemaRegistryIndex {
    // Index by fingerprint
    fp: MultiMap<FingerPrint, SchemaRef>,
    // Index by context and schema id
    ctx_schema_ref: MultiMap<ContextSchemaRef, SchemaRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ContextSchemaRef {
    context: ContextName,
    id: SchemaId,
}

#[derive(Debug, Clone)]
struct SchemaRef {
    context: ContextName,
    subject: SubjectName,
    version: SchemaVersion,
    id: SchemaId,
    schema_type: SchemaType,
    fingerprint: FingerPrint,
}

impl SchemaRegistryIndex {
    pub fn new() -> Self {
        Self {
            fp: MultiMap::new(),
            ctx_schema_ref: MultiMap::new(),
        }
    }

    pub fn index(
        &mut self,
        context: &ContextName,
        schema_subject: &Subject,
    ) -> anyhow::Result<()> {
        match schema_subject.schema_type {
            SchemaType::Avro => self.index_avro(context, schema_subject),
            SchemaType::Protobuf => Ok(()),
            SchemaType::Json => Ok(()),
        }
    }

    fn index_avro(
        &mut self,
        context: &ContextName,
        schema_subject: &Subject,
    ) -> anyhow::Result<()> {
        let schema = AvroSchema::parse_str(schema_subject.schema.as_str())?;

        let reference = SchemaRef {
            context: context.clone(),
            subject: schema_subject.subject.clone(),
            version: schema_subject.version.clone(),
            id: schema_subject.id.clone(),
            schema_type: schema_subject.schema_type.clone(),
            fingerprint: schema_subject.to_fingerprint()?,
        };

        self.insert(reference);

        Ok(())
    }


    fn insert(
        &mut self,
        reference: SchemaRef,
    ) {
        self.fp.insert(reference.fingerprint.clone(), reference.clone());

        let ctx_schema_ref = ContextSchemaRef {
            context: reference.context.clone(),
            id: reference.id.clone(),
        };

        self.ctx_schema_ref.insert(ctx_schema_ref, reference.clone());
    }

    pub fn find_by_fingerprint(&self, fingerprint: &FingerPrint) -> Option<&Vec<SchemaRef>> {
        self.fp.get_vec(fingerprint)
    }

    pub fn find_by_context_schema_ref(&self, context: &ContextName, id: &SchemaId) -> Option<&Vec<SchemaRef>> {
        self.ctx_schema_ref.get_vec(&ContextSchemaRef {
            context: context.clone(),
            id: id.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use clap::builder::TypedValueParser;

    use crate::context::ContextName;
    use crate::mapping::fingerprint::ToFingerPrint;
    use crate::mapping::index::{SchemaRef, SchemaRegistryIndex};
    use crate::registry::{SchemaId, SchemaType, SchemaVersion, Subject, SubjectName};

    fn schema_ref(ctx: &ContextName, subject: &Subject) -> SchemaRef {
        SchemaRef {
            context: ctx.clone(),
            subject: subject.subject.clone(),
            version: subject.version.clone(),
            id: subject.id.clone(),
            schema_type: subject.schema_type.clone(),
            fingerprint: subject.to_fingerprint()
                .expect("Failed to fingerprint schema"),
        }
    }

    #[test]
    fn find_avro_schema_by_fingerprint() {
        let mut index = SchemaRegistryIndex::new();
        let left = ContextName::new("left").unwrap();
        let right = ContextName::new("right").unwrap();
        let schema_subject = avrocado_subject();

        index.index(&left, &schema_subject).unwrap();
        index.index(&right, &schema_subject).unwrap();

        let expected = Some(&vec![
            schema_ref(&left, &schema_subject),
            schema_ref(&right, &schema_subject)
        ]);

        assert!(matches!(
            index.find_by_fingerprint(&schema_subject.to_fingerprint().unwrap()),
            expected));
    }

    #[test]
    fn index_protobuf_schema_is_ignored() {
        let mut index = SchemaRegistryIndex::new();
        let context = ContextName::new("fries").unwrap();
        let schema_subject = potatobuf_subject();

        index.index(&context, &schema_subject).unwrap();

        assert!(matches!(
            index.find_by_fingerprint(&schema_subject.to_fingerprint().unwrap()),
            None));
    }

    #[test]
    fn index_json_schema_is_ignored() {
        let mut index = SchemaRegistryIndex::new();
        let context = ContextName::new("low-hanging-jackson-fruit").unwrap();
        let schema_subject = jacksonfruit_subject();

        index.index(&context, &schema_subject).unwrap();

        assert!(matches!(
            index.find_by_fingerprint(&schema_subject.to_fingerprint().unwrap()),
            None));
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