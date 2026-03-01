// ThinkingLanguage — Schema Registry & Evolution
// Licensed under MIT OR Apache-2.0
//
// Phase 21: Versioned schema registry with compatibility checking.
// When the `native` feature is disabled (WASM builds), a minimal stub is provided.

#[cfg(feature = "native")]
mod native {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tl_data::{ArrowSchema, ArrowField, ArrowDataType};

    /// Runtime schema registry for versioned schemas.
    #[derive(Debug, Clone, Default)]
    pub struct SchemaRegistry {
        /// Map of schema name → list of (version, schema) ordered by version
        schemas: HashMap<String, Vec<VersionedSchema>>,
        /// Registered migrations: (schema, from_ver, to_ver) → ops
        migrations: HashMap<(String, i64, i64), Vec<MigrationOp>>,
    }

    /// A versioned schema entry.
    #[derive(Debug, Clone)]
    pub struct VersionedSchema {
        pub version: i64,
        pub schema: Arc<ArrowSchema>,
        pub metadata: SchemaMetadata,
    }

    /// Metadata about a versioned schema's fields.
    #[derive(Debug, Clone, Default)]
    pub struct SchemaMetadata {
        /// field name → version when added
        pub field_since: HashMap<String, i64>,
        /// field name → version when deprecated
        pub field_deprecated: HashMap<String, i64>,
        /// field name → default value as string
        pub field_defaults: HashMap<String, String>,
    }

    /// Compatibility mode for schema evolution checking.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CompatibilityMode {
        /// New schema can read old data (additions OK, removals NOT OK)
        Backward,
        /// Old schema can read new data (removals OK, additions with defaults OK)
        Forward,
        /// Both backward and forward compatible
        Full,
        /// No compatibility checking
        None,
    }

    /// Difference between two schema versions.
    #[derive(Debug, Clone, PartialEq)]
    pub enum SchemaDiff {
        FieldAdded { name: String, type_name: String },
        FieldRemoved { name: String },
        FieldRenamed { from: String, to: String },
        TypeChanged { field: String, from: String, to: String },
    }

    /// A compatibility issue found during checking.
    #[derive(Debug, Clone, PartialEq)]
    pub enum CompatIssue {
        FieldRemovedNotBackward(String),
        FieldAddedNoDefault(String),
        TypeNarrowed { field: String, from: String, to: String },
    }

    /// A migration operation stored in the registry.
    #[derive(Debug, Clone)]
    pub enum MigrationOp {
        AddColumn { name: String, type_name: String, default: Option<String> },
        DropColumn { name: String },
        RenameColumn { from: String, to: String },
        AlterType { column: String, new_type: String },
    }

    impl SchemaRegistry {
        pub fn new() -> Self {
            Self::default()
        }

        /// Register a versioned schema. Returns error if version already exists.
        pub fn register(&mut self, name: &str, version: i64, schema: Arc<ArrowSchema>, metadata: SchemaMetadata) -> Result<(), String> {
            let entries = self.schemas.entry(name.to_string()).or_default();
            if entries.iter().any(|e| e.version == version) {
                return Err(format!("Schema `{}` version {} already registered", name, version));
            }
            entries.push(VersionedSchema { version, schema, metadata });
            entries.sort_by_key(|e| e.version);
            Ok(())
        }

        /// Get a specific version of a schema.
        pub fn get(&self, name: &str, version: i64) -> Option<&VersionedSchema> {
            self.schemas.get(name)?.iter().find(|e| e.version == version)
        }

        /// Get the latest version of a schema.
        pub fn latest(&self, name: &str) -> Option<&VersionedSchema> {
            self.schemas.get(name)?.last()
        }

        /// Get all versions of a schema, ordered by version.
        pub fn history(&self, name: &str) -> Vec<&VersionedSchema> {
            self.schemas.get(name).map(|v| v.iter().collect()).unwrap_or_default()
        }

        /// Get list of version numbers for a schema.
        pub fn versions(&self, name: &str) -> Vec<i64> {
            self.schemas.get(name).map(|v| v.iter().map(|e| e.version).collect()).unwrap_or_default()
        }

        /// Get field names and types for a specific version.
        pub fn fields(&self, name: &str, version: i64) -> Vec<(String, String)> {
            if let Some(vs) = self.get(name, version) {
                vs.schema.fields().iter().map(|f| {
                    (f.name().to_string(), format!("{}", f.data_type()))
                }).collect()
            } else {
                Vec::new()
            }
        }

        /// Compute diff between two versions.
        pub fn diff(&self, name: &str, v1: i64, v2: i64) -> Vec<SchemaDiff> {
            let s1 = match self.get(name, v1) {
                Some(s) => s,
                None => return Vec::new(),
            };
            let s2 = match self.get(name, v2) {
                Some(s) => s,
                None => return Vec::new(),
            };

            let mut diffs = Vec::new();
            let old_fields: HashMap<&str, &ArrowField> = s1.schema.fields().iter().map(|f| (f.name().as_str(), f.as_ref())).collect();
            let new_fields: HashMap<&str, &ArrowField> = s2.schema.fields().iter().map(|f| (f.name().as_str(), f.as_ref())).collect();

            // Check for renames first (from registered migrations)
            let renames = self.get_renames(name, v1, v2);

            // Fields in old but not in new
            for (name_str, _field) in &old_fields {
                if !new_fields.contains_key(name_str) {
                    // Check if this was a rename
                    if let Some(new_name) = renames.get(*name_str) {
                        diffs.push(SchemaDiff::FieldRenamed {
                            from: name_str.to_string(),
                            to: new_name.clone(),
                        });
                    } else {
                        diffs.push(SchemaDiff::FieldRemoved { name: name_str.to_string() });
                    }
                }
            }

            // Fields in new but not in old
            for (name_str, field) in &new_fields {
                if !old_fields.contains_key(name_str) {
                    // Check if this was a rename target
                    let is_rename_target = renames.values().any(|v| v == *name_str);
                    if !is_rename_target {
                        diffs.push(SchemaDiff::FieldAdded {
                            name: name_str.to_string(),
                            type_name: format!("{}", field.data_type()),
                        });
                    }
                }
            }

            // Type changes (fields in both)
            for (name_str, old_field) in &old_fields {
                if let Some(new_field) = new_fields.get(name_str) {
                    if old_field.data_type() != new_field.data_type() {
                        diffs.push(SchemaDiff::TypeChanged {
                            field: name_str.to_string(),
                            from: format!("{}", old_field.data_type()),
                            to: format!("{}", new_field.data_type()),
                        });
                    }
                }
            }

            diffs
        }

        /// Check compatibility between two versions.
        pub fn check_compatibility(&self, name: &str, old_ver: i64, new_ver: i64, mode: CompatibilityMode) -> Vec<CompatIssue> {
            if mode == CompatibilityMode::None {
                return Vec::new();
            }

            let old_schema = match self.get(name, old_ver) {
                Some(s) => s,
                None => return Vec::new(),
            };
            let new_schema = match self.get(name, new_ver) {
                Some(s) => s,
                None => return Vec::new(),
            };

            let mut issues = Vec::new();
            let old_fields: HashMap<&str, &ArrowField> = old_schema.schema.fields().iter().map(|f| (f.name().as_str(), f.as_ref())).collect();
            let new_fields: HashMap<&str, &ArrowField> = new_schema.schema.fields().iter().map(|f| (f.name().as_str(), f.as_ref())).collect();

            // Backward compatibility: new must contain all old fields
            if mode == CompatibilityMode::Backward || mode == CompatibilityMode::Full {
                for (name_str, _) in &old_fields {
                    if !new_fields.contains_key(name_str) {
                        issues.push(CompatIssue::FieldRemovedNotBackward(name_str.to_string()));
                    }
                }
            }

            // Forward compatibility: new fields must have defaults
            if mode == CompatibilityMode::Forward || mode == CompatibilityMode::Full {
                for (name_str, _) in &new_fields {
                    if !old_fields.contains_key(name_str) {
                        // Check if field has a default in metadata
                        let has_default = new_schema.metadata.field_defaults.contains_key(*name_str);
                        if !has_default {
                            issues.push(CompatIssue::FieldAddedNoDefault(name_str.to_string()));
                        }
                    }
                }
            }

            // Type narrowing check (both directions)
            for (name_str, old_field) in &old_fields {
                if let Some(new_field) = new_fields.get(name_str) {
                    if old_field.data_type() != new_field.data_type() {
                        if !is_type_widening(old_field.data_type(), new_field.data_type()) {
                            issues.push(CompatIssue::TypeNarrowed {
                                field: name_str.to_string(),
                                from: format!("{}", old_field.data_type()),
                                to: format!("{}", new_field.data_type()),
                            });
                        }
                    }
                }
            }

            issues
        }

        /// Register a migration.
        pub fn register_migration(&mut self, schema_name: &str, from_ver: i64, to_ver: i64, ops: Vec<MigrationOp>) {
            self.migrations.insert((schema_name.to_string(), from_ver, to_ver), ops);
        }

        /// Get renames from registered migrations.
        fn get_renames(&self, name: &str, from_ver: i64, to_ver: i64) -> HashMap<String, String> {
            let mut renames = HashMap::new();
            if let Some(ops) = self.migrations.get(&(name.to_string(), from_ver, to_ver)) {
                for op in ops {
                    if let MigrationOp::RenameColumn { from, to } = op {
                        renames.insert(from.clone(), to.clone());
                    }
                }
            }
            renames
        }

        /// Apply a migration to produce a new schema version.
        pub fn apply_migration(&mut self, schema_name: &str, from_ver: i64, to_ver: i64, ops: &[MigrationOp]) -> Result<(), String> {
            let source = self.get(schema_name, from_ver)
                .ok_or_else(|| format!("Source schema `{}` v{} not found", schema_name, from_ver))?
                .clone();

            let mut fields: Vec<ArrowField> = source.schema.fields().iter().map(|f| f.as_ref().clone()).collect();
            let mut metadata = source.metadata.clone();

            for op in ops {
                match op {
                    MigrationOp::AddColumn { name, type_name, default } => {
                        let dt = type_name_to_arrow(type_name);
                        fields.push(ArrowField::new(name, dt, true));
                        metadata.field_since.insert(name.clone(), to_ver);
                        if let Some(def) = default {
                            metadata.field_defaults.insert(name.clone(), def.clone());
                        }
                    }
                    MigrationOp::DropColumn { name } => {
                        fields.retain(|f| f.name() != name);
                    }
                    MigrationOp::RenameColumn { from, to } => {
                        for f in &mut fields {
                            if f.name() == from {
                                *f = ArrowField::new(to, f.data_type().clone(), f.is_nullable());
                            }
                        }
                    }
                    MigrationOp::AlterType { column, new_type } => {
                        let dt = type_name_to_arrow(new_type);
                        for f in &mut fields {
                            if f.name() == column {
                                *f = ArrowField::new(column, dt.clone(), f.is_nullable());
                            }
                        }
                    }
                }
            }

            let new_schema = Arc::new(ArrowSchema::new(fields));
            self.register(schema_name, to_ver, new_schema, metadata)?;
            self.register_migration(schema_name, from_ver, to_ver, ops.to_vec());
            Ok(())
        }
    }

    /// Check if a type change is a widening (safe) operation.
    fn is_type_widening(from: &ArrowDataType, to: &ArrowDataType) -> bool {
        matches!((from, to),
            (ArrowDataType::Int8, ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64 | ArrowDataType::Float32 | ArrowDataType::Float64)
            | (ArrowDataType::Int16, ArrowDataType::Int32 | ArrowDataType::Int64 | ArrowDataType::Float32 | ArrowDataType::Float64)
            | (ArrowDataType::Int32, ArrowDataType::Int64 | ArrowDataType::Float64)
            | (ArrowDataType::Float32, ArrowDataType::Float64)
        )
    }

    /// Convert a type name string to Arrow DataType (public for VM use).
    pub fn type_name_to_arrow_pub(name: &str) -> ArrowDataType {
        type_name_to_arrow(name)
    }

    /// Convert a type name string to Arrow DataType.
    fn type_name_to_arrow(name: &str) -> ArrowDataType {
        match name {
            "int8" => ArrowDataType::Int8,
            "int16" => ArrowDataType::Int16,
            "int32" | "int" => ArrowDataType::Int32,
            "int64" => ArrowDataType::Int64,
            "float32" | "float" => ArrowDataType::Float32,
            "float64" => ArrowDataType::Float64,
            "string" | "utf8" | "Utf8" => ArrowDataType::Utf8,
            "bool" | "boolean" => ArrowDataType::Boolean,
            _ => ArrowDataType::Utf8, // fallback
        }
    }

    impl CompatibilityMode {
        pub fn from_str(s: &str) -> Self {
            match s.to_lowercase().as_str() {
                "backward" | "backwards" => CompatibilityMode::Backward,
                "forward" | "forwards" => CompatibilityMode::Forward,
                "full" => CompatibilityMode::Full,
                "none" => CompatibilityMode::None,
                _ => CompatibilityMode::Backward,
            }
        }
    }

    impl std::fmt::Display for SchemaDiff {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                SchemaDiff::FieldAdded { name, type_name } => write!(f, "added field `{}` ({})", name, type_name),
                SchemaDiff::FieldRemoved { name } => write!(f, "removed field `{}`", name),
                SchemaDiff::FieldRenamed { from, to } => write!(f, "renamed field `{}` to `{}`", from, to),
                SchemaDiff::TypeChanged { field, from, to } => write!(f, "changed type of `{}` from {} to {}", field, from, to),
            }
        }
    }

    impl std::fmt::Display for CompatIssue {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                CompatIssue::FieldRemovedNotBackward(name) => write!(f, "field `{}` removed (breaks backward compat)", name),
                CompatIssue::FieldAddedNoDefault(name) => write!(f, "field `{}` added without default (breaks forward compat)", name),
                CompatIssue::TypeNarrowed { field, from, to } => write!(f, "field `{}` type narrowed from {} to {}", field, from, to),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn make_schema(fields: &[(&str, ArrowDataType)]) -> Arc<ArrowSchema> {
            let arrow_fields: Vec<ArrowField> = fields.iter().map(|(n, dt)| ArrowField::new(*n, dt.clone(), true)).collect();
            Arc::new(ArrowSchema::new(arrow_fields))
        }

        #[test]
        fn test_register_schema_v1() {
            let mut reg = SchemaRegistry::new();
            let schema = make_schema(&[("id", ArrowDataType::Int64), ("name", ArrowDataType::Utf8)]);
            assert!(reg.register("User", 1, schema, SchemaMetadata::default()).is_ok());
            assert!(reg.get("User", 1).is_some());
        }

        #[test]
        fn test_register_schema_v2() {
            let mut reg = SchemaRegistry::new();
            let s1 = make_schema(&[("id", ArrowDataType::Int64)]);
            let s2 = make_schema(&[("id", ArrowDataType::Int64), ("email", ArrowDataType::Utf8)]);
            reg.register("User", 1, s1, SchemaMetadata::default()).unwrap();
            reg.register("User", 2, s2, SchemaMetadata::default()).unwrap();
            assert!(reg.get("User", 2).is_some());
        }

        #[test]
        fn test_get_specific_version() {
            let mut reg = SchemaRegistry::new();
            let s1 = make_schema(&[("id", ArrowDataType::Int64)]);
            let s2 = make_schema(&[("id", ArrowDataType::Int64), ("name", ArrowDataType::Utf8)]);
            reg.register("User", 1, s1, SchemaMetadata::default()).unwrap();
            reg.register("User", 2, s2, SchemaMetadata::default()).unwrap();
            let v1 = reg.get("User", 1).unwrap();
            assert_eq!(v1.schema.fields().len(), 1);
            let v2 = reg.get("User", 2).unwrap();
            assert_eq!(v2.schema.fields().len(), 2);
        }

        #[test]
        fn test_get_latest() {
            let mut reg = SchemaRegistry::new();
            let s1 = make_schema(&[("id", ArrowDataType::Int64)]);
            let s2 = make_schema(&[("id", ArrowDataType::Int64), ("name", ArrowDataType::Utf8)]);
            reg.register("User", 1, s1, SchemaMetadata::default()).unwrap();
            reg.register("User", 2, s2, SchemaMetadata::default()).unwrap();
            let latest = reg.latest("User").unwrap();
            assert_eq!(latest.version, 2);
        }

        #[test]
        fn test_history_ordered() {
            let mut reg = SchemaRegistry::new();
            let s1 = make_schema(&[("id", ArrowDataType::Int64)]);
            let s2 = make_schema(&[("id", ArrowDataType::Int64), ("name", ArrowDataType::Utf8)]);
            let s3 = make_schema(&[("id", ArrowDataType::Int64), ("name", ArrowDataType::Utf8), ("email", ArrowDataType::Utf8)]);
            reg.register("User", 1, s1, SchemaMetadata::default()).unwrap();
            reg.register("User", 3, s3, SchemaMetadata::default()).unwrap();
            reg.register("User", 2, s2, SchemaMetadata::default()).unwrap();
            let hist = reg.history("User");
            let versions: Vec<i64> = hist.iter().map(|v| v.version).collect();
            assert_eq!(versions, vec![1, 2, 3]);
        }

        #[test]
        fn test_backward_compat_adding_column_ok() {
            let mut reg = SchemaRegistry::new();
            let s1 = make_schema(&[("id", ArrowDataType::Int64)]);
            let s2 = make_schema(&[("id", ArrowDataType::Int64), ("name", ArrowDataType::Utf8)]);
            reg.register("User", 1, s1, SchemaMetadata::default()).unwrap();
            reg.register("User", 2, s2, SchemaMetadata::default()).unwrap();
            let issues = reg.check_compatibility("User", 1, 2, CompatibilityMode::Backward);
            assert!(issues.is_empty(), "Adding column should be backward compatible, got: {:?}", issues);
        }

        #[test]
        fn test_backward_compat_removing_column_fails() {
            let mut reg = SchemaRegistry::new();
            let s1 = make_schema(&[("id", ArrowDataType::Int64), ("name", ArrowDataType::Utf8)]);
            let s2 = make_schema(&[("id", ArrowDataType::Int64)]);
            reg.register("User", 1, s1, SchemaMetadata::default()).unwrap();
            reg.register("User", 2, s2, SchemaMetadata::default()).unwrap();
            let issues = reg.check_compatibility("User", 1, 2, CompatibilityMode::Backward);
            assert!(!issues.is_empty());
            assert!(matches!(&issues[0], CompatIssue::FieldRemovedNotBackward(n) if n == "name"));
        }

        #[test]
        fn test_backward_compat_type_widening_ok() {
            let mut reg = SchemaRegistry::new();
            let s1 = make_schema(&[("id", ArrowDataType::Int32)]);
            let s2 = make_schema(&[("id", ArrowDataType::Int64)]);
            reg.register("T", 1, s1, SchemaMetadata::default()).unwrap();
            reg.register("T", 2, s2, SchemaMetadata::default()).unwrap();
            let issues = reg.check_compatibility("T", 1, 2, CompatibilityMode::Backward);
            assert!(issues.is_empty(), "Type widening Int32->Int64 should be backward compatible");
        }

        #[test]
        fn test_backward_compat_type_narrowing_fails() {
            let mut reg = SchemaRegistry::new();
            let s1 = make_schema(&[("id", ArrowDataType::Int64)]);
            let s2 = make_schema(&[("id", ArrowDataType::Int32)]);
            reg.register("T", 1, s1, SchemaMetadata::default()).unwrap();
            reg.register("T", 2, s2, SchemaMetadata::default()).unwrap();
            let issues = reg.check_compatibility("T", 1, 2, CompatibilityMode::Backward);
            assert!(!issues.is_empty());
            assert!(matches!(&issues[0], CompatIssue::TypeNarrowed { .. }));
        }

        #[test]
        fn test_forward_compat_removing_column_ok() {
            let mut reg = SchemaRegistry::new();
            let s1 = make_schema(&[("id", ArrowDataType::Int64), ("name", ArrowDataType::Utf8)]);
            let s2 = make_schema(&[("id", ArrowDataType::Int64)]);
            reg.register("User", 1, s1, SchemaMetadata::default()).unwrap();
            reg.register("User", 2, s2, SchemaMetadata::default()).unwrap();
            let issues = reg.check_compatibility("User", 1, 2, CompatibilityMode::Forward);
            assert!(issues.is_empty(), "Removing column should be forward compatible");
        }

        #[test]
        fn test_forward_compat_adding_without_default_fails() {
            let mut reg = SchemaRegistry::new();
            let s1 = make_schema(&[("id", ArrowDataType::Int64)]);
            let s2 = make_schema(&[("id", ArrowDataType::Int64), ("name", ArrowDataType::Utf8)]);
            reg.register("User", 1, s1, SchemaMetadata::default()).unwrap();
            reg.register("User", 2, s2, SchemaMetadata::default()).unwrap();
            let issues = reg.check_compatibility("User", 1, 2, CompatibilityMode::Forward);
            assert!(!issues.is_empty());
            assert!(matches!(&issues[0], CompatIssue::FieldAddedNoDefault(n) if n == "name"));
        }

        #[test]
        fn test_full_compat() {
            let mut reg = SchemaRegistry::new();
            let s1 = make_schema(&[("id", ArrowDataType::Int32)]);
            let mut meta = SchemaMetadata::default();
            meta.field_defaults.insert("name".to_string(), "\"\"".to_string());
            let s2 = make_schema(&[("id", ArrowDataType::Int64), ("name", ArrowDataType::Utf8)]);
            reg.register("T", 1, s1, SchemaMetadata::default()).unwrap();
            reg.register("T", 2, s2, meta).unwrap();
            let issues = reg.check_compatibility("T", 1, 2, CompatibilityMode::Full);
            assert!(issues.is_empty(), "Type widening + defaults should pass full compat, got: {:?}", issues);
        }

        #[test]
        fn test_diff_additions_removals() {
            let mut reg = SchemaRegistry::new();
            let s1 = make_schema(&[("id", ArrowDataType::Int64), ("old_field", ArrowDataType::Utf8)]);
            let s2 = make_schema(&[("id", ArrowDataType::Int64), ("new_field", ArrowDataType::Utf8)]);
            reg.register("User", 1, s1, SchemaMetadata::default()).unwrap();
            reg.register("User", 2, s2, SchemaMetadata::default()).unwrap();
            let diffs = reg.diff("User", 1, 2);
            assert!(diffs.iter().any(|d| matches!(d, SchemaDiff::FieldRemoved { name } if name == "old_field")));
            assert!(diffs.iter().any(|d| matches!(d, SchemaDiff::FieldAdded { name, .. } if name == "new_field")));
        }

        #[test]
        fn test_duplicate_version_error() {
            let mut reg = SchemaRegistry::new();
            let s1 = make_schema(&[("id", ArrowDataType::Int64)]);
            let s2 = make_schema(&[("id", ArrowDataType::Int64)]);
            reg.register("User", 1, s1, SchemaMetadata::default()).unwrap();
            let result = reg.register("User", 1, s2, SchemaMetadata::default());
            assert!(result.is_err());
        }
    }
}

#[cfg(feature = "native")]
pub use native::*;

// Stub schema registry for WASM builds (no Arrow dependency)
#[cfg(not(feature = "native"))]
#[derive(Debug, Clone, Default)]
pub struct SchemaRegistry;

#[cfg(not(feature = "native"))]
impl SchemaRegistry {
    pub fn new() -> Self { SchemaRegistry }
}
