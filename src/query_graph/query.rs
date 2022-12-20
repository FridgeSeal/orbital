use fnv::FnvHashMap;
use prql_compiler::{parse, semantic::resolve, translate};
use shrinkwraprs::Shrinkwrap;
use smartstring::alias::String;
use std::collections::HashSet;
use std::hash::Hash;
use std::{collections::HashMap, ops::Deref};
use xxhash_rust::xxh3::{xxh3_64, Xxh3Builder};

#[derive(Debug, Shrinkwrap, Eq, PartialEq, Hash, Clone, Copy, Default, PartialOrd, Ord)]
struct QueryId(u64);

#[derive(Debug, Shrinkwrap, Eq, PartialEq, Hash, Clone, Default, PartialOrd, Ord)]
struct TableName(String);

#[derive(Debug, shrinkwraprs::Shrinkwrap, PartialEq, Eq, Hash)]
struct QueryName(String);

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum QueryKind {
    Query(Query),
    TableQuery(TableQuery),
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TableQuery {
    /// A structure used to define pre-existing, or fixed, tables already present within our data source.
    /// dbt calls the seeds/sources.
    id: QueryId,
    name: TableName,
}
#[derive(Debug)]
pub struct Query {
    id: QueryId,
    name: QueryName,
    resolved_query: prql_compiler::ast::rq::Query,
    dependencies: Vec<TableName>,
}

impl PartialEq for Query {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.resolved_query == other.resolved_query
            && self.dependencies == other.dependencies
    }
}

impl Eq for Query {}

impl Hash for Query {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.name.hash(state);
        // self.resolved_query.hash(state);
        self.dependencies.hash(state);
    }
}

#[derive(Debug)]
struct RawQuery {
    query_string: String,
    name: String,
}

pub type QueryMap<K, V> = HashMap<K, V, Xxh3Builder>;

#[derive(Debug)]
pub struct ResourceIdMap<T: Eq + Hash> {
    inner: QueryMap<String, T>,
    reverse: fnv::FnvHashMap<T, String>,
}

#[derive(Debug)]
pub struct QueryCollection {
    // Project? Scope? Might be better names
    query_map: QueryMap<String, QueryKind>,
    query_id_map: ResourceIdMap<QueryId>,
}

impl Query {
    fn new(
        id: QueryId,
        name: &str,
        parsed_query: prql_compiler::ast::rq::Query,
        dependencies: Vec<TableName>,
    ) -> Self {
        Self {
            id,
            name: QueryName(name.into()),
            resolved_query: parsed_query,
            dependencies,
        }
    }
}

impl<T: Eq + Hash + Copy + Default + Ord> ResourceIdMap<T> {
    pub fn new() -> Self {
        Self {
            inner: QueryMap::default(),
            reverse: FnvHashMap::default(),
        }
    }

    pub fn insert_resource(&mut self, resource_name: String, resource_id: T) {
        self.inner.insert(resource_name.clone(), resource_id);
        self.reverse.insert(resource_id, resource_name);
    }

    // pub fn remove_query(&mut self, resource_name: &impl AsRef<str>, resource_id: impl AsRef<T>) {
    //     self.inner.remove(resource_name.as_ref());
    //     self.reverse.remove(resource_id.as_ref());
    // }

    // pub fn remove_query_by_id(&mut self, resource_id: impl AsRef<T>) {
    //     if let Some((_, query_name)) = self.reverse.remove_entry(resource_id.as_ref()) {
    //         self.inner.remove(&query_name);
    //     }
    // }

    // pub fn remove_query_by_name(&mut self, query_name: &impl AsRef<str>) {
    //     if let Some((_, query_id)) = self.inner.remove_entry(query_name.as_ref()) {
    //         self.reverse.remove(&query_id);
    //     }
    // }

    // pub fn get_query_name(&self, resource_id: impl AsRef<T>) -> Option<&String> {
    //     self.reverse.get(resource_id.as_ref())
    // }
    // pub fn get_query_id(&self, resource_name: &impl AsRef<str>) -> Option<&T> {
    //     self.inner.get(resource_name.as_ref())
    // }
}

// impl ResourceIdMap<QueryId> {}

impl QueryCollection {
    pub fn new() -> Self {
        Self {
            query_map: QueryMap::default(),
            query_id_map: ResourceIdMap::new(),
        }
    }

    fn add_queries(&mut self, queries: Vec<RawQuery>) {
        let parsed_queries: Vec<_> = queries
            .iter()
            .filter_map(|q| self.prepare_query(&q.query_string, &q.name).ok())
            .collect();
        // 1st Iteration to build query-name -> query, query_name <--> query_id lookups
        for q in parsed_queries {
            self.query_id_map
                .insert_resource(q.name.clone(), q.id.clone());
            self.query_map.insert(q.name.clone(), QueryKind::Query(q));
        }
        let table_names: HashSet<_> = self
            .query_map
            .values()
            .filter_map(|f| match f {
                QueryKind::TableQuery(_) => None,
                QueryKind::Query(x) => Some(x),
            })
            .flat_map(|x| {
                x.dependencies
                    .iter()
                    .filter(|&y| !self.query_map.contains_key(y.deref()))
            })
            .map(|x| x.to_owned())
            .collect();
        table_names.into_iter().for_each(|t| {
            let name = t.deref();
            let id = QueryId(xxh3_64(t.deref().as_bytes()));
            let tbl = TableQuery {
                name: TableName(name.clone()),
                id: id.clone(),
            };
            self.query_id_map.insert_resource(name.clone(), id.clone());
            self.query_map
                .insert(name.clone(), QueryKind::TableQuery(tbl));
        });
    }

    /*
    raw query info --> collection of raw queries --> collection of parsed + identified queries
    ---> collection of queries + dag built
    */
    pub fn prepare_query(
        &self,
        raw_query: &str,
        query_name: impl AsRef<str>,
    ) -> Result<Query, Box<dyn std::error::Error>> {
        let parsed_query = resolve(parse(raw_query)?)?;
        let dependent_table_names = extract_dependent_tables(&parsed_query);
        let query_id = QueryId(xxh3_64(query_name.as_ref().as_bytes()));
        let query = Query::new(
            query_id,
            query_name.as_ref(),
            parsed_query,
            dependent_table_names,
        );
        Ok(query)
    }
}

impl Deref for QueryCollection {
    type Target = QueryMap<String, QueryKind>;

    fn deref(&self) -> &Self::Target {
        &self.query_map
    }
}

fn extract_dependent_tables(query: &prql_compiler::ast::rq::Query) -> Vec<TableName> {
    query
        .tables
        .iter()
        .filter_map(|t| t.name.clone())
        .map(|x| TableName(x.into()))
        .collect()
}

#[cfg(test)]
mod test_super {

    use super::*;

    #[test]
    fn test_can_parse_plain_sql() {
        let prql = "from employees | filter age > 35 | select name";
        let sql = prql_compiler::compile(prql)
            .unwrap()
            .replace("\n", " ")
            .replace("   ", " ");
        assert_eq!(sql, "SELECT name FROM employees WHERE age > 35")
    }

    #[test]
    fn test_can_get_table_names_from_prql() {
        let prql = "from employees | filter age > 35 | select name";
        let parsed = parse(prql).unwrap();
        let resolved_tables = resolve(parsed).unwrap().tables;
        let actual_names: Vec<_> = resolved_tables.iter().map(|t| t.name.clone()).collect();
        let expected_names = vec![Some("employees".to_string())];
        assert_eq!(actual_names, expected_names);
    }

    #[test]
    fn test_can_get_multiple_table_names_from_prql() {
        let prql = r#"from employees
        filter age > 35
        join side:inner other_table [==employee_id]
        select [name, employee_id, workplace]"#;
        let parsed = parse(prql).unwrap();
        let resolved_tables = resolve(parsed).unwrap().tables;
        let actual_names: Vec<_> = resolved_tables.iter().map(|t| t.name.clone()).collect();
        let expected_names = vec![
            Some("employees".to_string()),
            Some("other_table".to_string()),
        ];
        assert_eq!(actual_names, expected_names);
    }

    #[test]
    fn test_can_gen_clickhouse_query() {
        let prql = r#"prql dialect:clickhouse
        from employees
        filter age > 35
        join side:inner other_table [==employee_id]
        select [name, employee_id, workplace]"#;
        let parsed = prql_compiler::compile(prql)
            .unwrap()
            .replace("\n", " ")
            .replace("   ", " ");
        dbg!(parsed);
    }

    #[test]
    fn test_can_re_map_variables_in_query() {
        let prql = r#"prql dialect:clickhouse
        from employees
        filter age > 35
        filter location != 'Melbourne'
        join side:inner other_table [==employee_id]
        select [name, employee_id, workplace]"#;
        let parsed = parse(prql).unwrap();
        let mut resolved = resolve(parsed).unwrap();
        let my_var = "Melbourne";
        let var_collection: HashMap<_, _> = {
            let mut x = HashMap::new();
            x.insert("my_var".to_string(), my_var.to_string());
            x
        };
        if let Some(x) = resolved.relation.as_pipeline_mut() {
            x.iter_mut().filter_map(|t| t.as_from_mut()).for_each(|y| {
                y.columns.iter_mut().for_each(|c| {
                    if let Some((Some(z), _)) = c.kind.as_expr_mut() {
                        if let Some(alpha) = var_collection.get(z) {
                            *z = alpha.to_string();
                        }
                    };
                });
            });
        };
        dbg!(&resolved.relation);
        println!("{:?}", translate(resolved));
    }

    #[test]
    fn test_can_add_queries() {
        let queries = vec![RawQuery{name:"q1".into(), query_string: "from arcana | filter source != 'necronomicron'".into()},
        RawQuery{name:"q2".into(), query_string: "from rituals | join side:inner q1 [==source]".into(),},
        RawQuery{name:"q3".into(), query_string: "from rituals | derive [ritual_cost = component_count + price]  | sort ritual_cost".into(),}];
        let mut collection = QueryCollection::new();
        collection.add_queries(queries);
        dbg!(&collection.query_id_map);
        assert_eq!(collection.query_map.len(), 5);
        assert_eq!(
            collection.query_id_map.inner.keys().collect::<HashSet<_>>(),
            vec![
                "q1".into(),
                "arcana".into(),
                "q3".into(),
                "rituals".into(),
                "q2".into()
            ]
            .iter()
            .collect::<HashSet<_>>()
        );
    }

    #[test]
    fn test_query_dependency_registers_properly() {
        let queries = vec![RawQuery{name:"q1".into(), query_string: "from arcana | filter source != 'necronomicron'".into()},
        RawQuery{name:"q2".into(), query_string: "from rituals | join side:inner q1 [==source]".into(),},
        RawQuery{name:"q3".into(), query_string: "from rituals | derive [ritual_cost = component_count + price]  | sort ritual_cost".into(),}];
        let mut collection = QueryCollection::new();
        collection.add_queries(queries);
        dbg!(&collection.query_id_map);
        let q2 = collection.query_map.get("q2");
        assert!(matches!(q2, Some(QueryKind::Query(_))));
        let q2 = match q2 {
            Some(QueryKind::Query(x)) => Some(x),
            _ => None,
        }
        .unwrap();
        let mut q2_dependencies = q2.dependencies.clone();
        q2_dependencies.sort();
        let expected_deps = vec![TableName("q1".into()), TableName("rituals".into())];
        assert_eq!(q2_dependencies, expected_deps);
    }

    #[test]
    fn test_can_add_queries_incrementally() {
        let queries = vec![RawQuery {
            name: "q1".into(),
            query_string: "from arcana | filter source != 'necronomicron'".into(),
        }];
        let queries2 = vec![
        RawQuery{name:"q2".into(), query_string: "from rituals | join side:inner q1 [==source]".into(),},
        RawQuery{name:"q3".into(), query_string: "from rituals | derive [ritual_cost = component_count + price]  | sort ritual_cost".into(),}];
        let mut collection = QueryCollection::new();
        collection.add_queries(queries);
        dbg!(&collection.query_id_map);
        assert_eq!(collection.query_map.len(), 2);
        collection.add_queries(queries2);
        assert_eq!(collection.query_map.len(), 5);
        assert_eq!(
            collection.query_id_map.inner.keys().collect::<HashSet<_>>(),
            vec![
                "q1".into(),
                "arcana".into(),
                "q3".into(),
                "rituals".into(),
                "q2".into()
            ]
            .iter()
            .collect::<HashSet<_>>()
        );
    }
}
