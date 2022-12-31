use self::{
    graph::{QueryGraph, ValidGraphData},
    query::{QueryCollection, QueryId, RawQuery},
};
use std::ops::Deref;

pub mod graph;
pub mod query;

pub struct GraphMeta {
    graph: QueryGraph,
    query: QueryCollection,
}

impl GraphMeta {
    pub fn new(query_collection: QueryCollection) -> Option<Self> {
        generate_graph_from_collection(&query_collection).and_then(|graph| {
            Some(Self {
                graph,
                query: query_collection,
            })
        })
    }
}

fn generate_graph_from_collection(c: &QueryCollection) -> Option<QueryGraph> {
    let edges: Vec<_> = c
        .values()
        .map(|node| (node.id(), c.get_query_depedencies(node.name())))
        .flat_map(|(id, deps)| gen_edge_pairs(id, &deps))
        .collect();
    ValidGraphData::new_from_edges(&edges).and_then(QueryGraph::new_from_valid_data)
}

fn gen_edge_pairs(src_node: &QueryId, node_deps: &[QueryId]) -> Vec<(u64, u64)> {
    node_deps
        .iter()
        .map(|n| (n, src_node))
        .map(|(src, dst)| (src.deref().to_owned(), dst.deref().to_owned()))
        .collect()
}

#[cfg(test)]
mod test_query_graph {
    use petgraph::dot;

    use super::*;

    #[test]
    fn test_can_generate_graph_from_queries() {
        let queries = vec![
            RawQuery::new("q1", "from arcana | filter source != 'necronomicron'"),
            RawQuery::new("q2", "from rituals | join side:inner q1 [==source]"),
            RawQuery::new("q3", "from q2 | filter something == 'blah'"),
            RawQuery::new(
                "q4",
                "from q3 | join side:inner rituals [==source] | join side:inner q1 [==other]",
            ),
        ];
        let mut collection = QueryCollection::new();
        collection.add_queries(queries);
        let query_graph = generate_graph_from_collection(&collection).unwrap();
        dbg!(collection.query_id_map.inner);
        println!(
            "{:?}",
            petgraph::dot::Dot::with_config(&query_graph.inner, &[dot::Config::EdgeNoLabel])
        );
    }
}
