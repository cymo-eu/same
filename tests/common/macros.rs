#[macro_export]
macro_rules! reference {
    ($name:expr, $subject:expr) => {
        same::registry::SchemaReference {
            name: $name.to_string(),
            subject: $subject.subject.to_string(),
            version: $subject.version,
        }
    };
    () => {};
}