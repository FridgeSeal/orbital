use figment::{
    providers::{Format, Yaml},
    Figment,
};
use serde::Deserialize;
use smartstring::alias::String;
use std::{collections::HashMap, path::PathBuf};
use version_rs::Version;

type ResourceMetadata = HashMap<String, String>;

#[derive(Debug, Deserialize)]
struct Project {
    name: ResourceName,
    version: Version,
    model_path: PathBuf,
    seed_path: PathBuf,
    clean_targets: PathBuf,
    log_path: PathBuf,
    models: Vec<ResourceConfig>,
    seeds: Vec<ResourceConfig>,
    sources: Vec<SourceConfig>,
    vars: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct ResourceConfig {
    name: ResourceName,
    enabled: bool,
    database: ResourceName,
    schema: ResourceName,
    exclude_full_refresh: bool,
    metadata: Option<ResourceMetadata>,
}

prae::define! {
    #[derive(Debug, Deserialize)]
    pub ResourceName: String;
    ensure |name| !name.is_empty();
}

#[derive(Debug, Deserialize)]
struct ResourceProperties {
    name: ResourceName,          // Presumably, must match ResourceConfig.nam?
    description: Option<String>, // Is there a crate for parsed markdown?
    config: ResourceConfig,
    //tests:,
    columns: Vec<ColumnMetada>,
}

#[derive(Debug, Deserialize)]
struct ColumnMetada {
    name: ResourceName,
    description: Option<String>, // Same point about markdown here too
    quote: bool,
    // column_type: ??? // Surely
}

#[derive(Debug, Deserialize)]
struct SourceConfig {
    name: ResourceName,
    enabled: bool,
}

#[derive(Debug, Deserialize)]
struct SourceProperties {
    name: ResourceName,
    database: ResourceName,
    schema: ResourceName,
    // tables: HashMap<String, TableProperties>,
    meta: Option<ResourceMetadata>,
}

#[derive(Debug, Deserialize)]
struct Freshness {
    loaded_at_field: FullyQualifiedColumn,
    warn_after: FreshnessThreshold,
    error_after: FreshnessThreshold,
    filter: Option<String>, // Filter clause, probably best expressed as a typed "Expression" a-la Polars and co
}

#[derive(Debug, Deserialize)]
pub struct FullyQualifiedTable {
    database: ResourceName,
    schema: ResourceName,
    table: ResourceName,
}

#[derive(Debug, Deserialize)]
pub struct FullyQualifiedColumn {
    table: FullyQualifiedTable,
    column: ResourceName,
}

#[derive(Debug, Deserialize)]
struct FreshnessThreshold {
    count: u32,
    period: FreshnessPeriod,
}

#[derive(Debug, Deserialize)]
enum FreshnessPeriod {
    Hour,
    Day,
    Month,
}
