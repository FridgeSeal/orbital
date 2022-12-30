use fnv::{FnvHashMap, FnvHashSet};
use petgraph::Direction;
use petgraph::{
    algo,
    prelude::DiGraph,
    stable_graph::{IndexType, NodeIndex},
    Directed, Graph,
};

type NodeId = u64;
type IxType = u8;
type IdLookupTable = FnvHashMap<NodeId, NodeIndex<IxType>>;
pub struct QueryGraph {
    pub inner: DiGraph<NodeId, (), IxType>,
    lookup_table: IdLookupTable,
}

impl QueryGraph {
    pub fn new_from_valid_data(valid_data: ValidGraphData) -> Option<Self> {
        let dag: DiAcylcicGraph = valid_data.into();
        if algo::is_cyclic_directed(&dag.raw_graph) {
            return None;
        };
        Some(QueryGraph {
            inner: dag.raw_graph,
            lookup_table: dag.lookup_table,
        })
    }
    pub fn new_from_edges(edges: Vec<(NodeId, NodeId)>) -> Option<Self> {
        ValidGraphData::new_from_edges(&edges).and_then(QueryGraph::new_from_valid_data)
    }

    pub fn new_from_ids_and_edges(
        node_ids: Vec<NodeId>,
        edges: Vec<(NodeId, NodeId)>,
    ) -> Option<Self> {
        ValidGraphData::new_from_id_edge_pairs(&node_ids, &edges)
            .and_then(QueryGraph::new_from_valid_data)
    }

    pub fn get_index(&self, node_id: NodeId) -> Option<NodeIndex<IxType>> {
        self.lookup_table.get(&node_id).copied()
    }

    pub fn get_id(&self, node_index: NodeIndex<IxType>) -> Option<NodeId> {
        self.lookup_table
            .iter()
            .find(|(_k, &v)| v == node_index)
            .map(|(k, _)| k)
            .copied()
    }

    pub fn get_root_nodes(&self) -> Vec<NodeId> {
        self.inner
            .externals(Direction::Incoming)
            .filter_map(|n_idx| self.get_id(n_idx))
            .collect()
    }
}
struct DiAcylcicGraph {
    raw_graph: DiGraph<NodeId, (), IxType>,
    lookup_table: IdLookupTable,
}

impl From<ValidGraphData> for DiAcylcicGraph {
    fn from(v: ValidGraphData) -> Self {
        let mut g: Graph<NodeId, (), Directed, IxType> =
            DiGraph::with_capacity(v.nodes.len(), v.edges.len());
        let node_id_map: FnvHashMap<_, _> = v
            .nodes
            .into_iter()
            .map(|n| {
                let idx = g.add_node(n);
                (n, idx)
            })
            .collect();
        v.edges.into_iter().for_each(|(src, dest)| {
            let idx_a = node_id_map[&src];
            let idx_b = node_id_map[&dest];
            g.update_edge(idx_a, idx_b, ());
        });
        Self {
            raw_graph: g,
            lookup_table: node_id_map,
        }
    }
}

struct ValidGraphData {
    nodes: Vec<u8>,
    edges: Vec<(u8, u8)>,
}

impl ValidGraphData {
    pub fn new_from_id_edge_pairs(node_ids: &[NodeId], edges: &[(NodeId, NodeId)]) -> Option<Self> {
        // Ensure edges only refer to nodes present in the node_ids
        let distinct_edge_ids = edges
            .iter()
            .fold(FnvHashSet::default(), |mut acc, (src, dest)| {
                acc.insert(src);
                acc.insert(dest);
                acc
            });
        let node_id_set: FnvHashSet<&u8> = node_ids.iter().collect();
        let mismatches: Vec<_> = distinct_edge_ids.difference(&node_id_set).collect();
        if !mismatches.is_empty() {
            println!("Invalid dataset provided. Edge data refers to un-referenced nodes!");
            println!("Offending nodes found in edges: {:?}", mismatches);
            return None;
        }
        let orphan_nodes: FnvHashSet<_> = node_id_set.difference(&distinct_edge_ids).collect();
        if !orphan_nodes.is_empty() {
            println!("Orphan nodes detected - removing. {:?}", orphan_nodes);
        }
        let valid_nodes = {
            let mut nodes: Vec<_> = node_id_set
                .iter()
                .filter(|i| !orphan_nodes.contains(i))
                .map(|&x| *x)
                .collect();
            nodes.sort_unstable();
            nodes
        };
        if valid_nodes.is_empty() || node_id_set.is_empty() {
            println!("Invalid data");
            return None;
        };
        let valid_graph_data = Self {
            nodes: valid_nodes,
            edges: edges.to_vec(),
        };
        Some(valid_graph_data)
    }

    pub fn new_from_edges(edges: &[(NodeId, NodeId)]) -> Option<Self> {
        let node_ids: Vec<_> = edges
            .iter()
            .fold(FnvHashSet::default(), |mut acc, (src, dest)| {
                acc.insert(src);
                acc.insert(dest);
                acc
            })
            .iter()
            .map(|x| **x)
            .collect();
        ValidGraphData::new_from_id_edge_pairs(&node_ids, edges)
    }
}

fn find_orphan_nodes<X, Y: IndexType>(g: &Graph<X, (), Directed, Y>) -> Vec<NodeIndex<Y>> {
    let orphan_nodes: Vec<_> = g
        .externals(Direction::Outgoing)
        .filter(|ext| g.edges_directed(*ext, Direction::Incoming).next().is_none())
        .collect();
    orphan_nodes
}

#[cfg(test)]
mod test_graphs {

    use petgraph::{
        dot,
        graph::DiGraph,
        stable_graph::{node_index, NodeIndex},
    };

    use super::*;

    #[test]
    fn test_dag_construction() {
        let g = DiGraph::<u16, ()>::from_edges(&[(0, 1), (0, 2), (3, 2), (2, 4), (4, 5)]);
        let output = format!(
            "{:?}",
            dot::Dot::with_config(&g, &[dot::Config::NodeIndexLabel, dot::Config::EdgeNoLabel])
        );
        assert_eq!(
            output,
            r#"digraph {
    0 [ label = "0" ]
    1 [ label = "1" ]
    2 [ label = "2" ]
    3 [ label = "3" ]
    4 [ label = "4" ]
    5 [ label = "5" ]
    0 -> 1 [ ]
    0 -> 2 [ ]
    3 -> 2 [ ]
    2 -> 4 [ ]
    4 -> 5 [ ]
}
"#
        );
    }

    #[test]
    fn test_detecting_cycles() {
        use petgraph::algo;
        let g = DiGraph::<u16, ()>::from_edges(&[(0, 1), (0, 2), (3, 2), (2, 4), (4, 5)]);
        let contains_cycle = algo::is_cyclic_directed(&g);
        assert_eq!(contains_cycle, false);
    }

    #[test]
    fn test_topological_sorted_dag() {
        let g = DiGraph::<u16, ()>::from_edges(&[(0, 1), (0, 2), (3, 2), (2, 4), (4, 5)]);
        let g_sorted = algo::toposort(&g, None);
        assert!(g_sorted.is_ok())
    }

    #[test]
    fn test_get_dependent_nodes() {
        let g = DiGraph::<u16, ()>::from_edges(&[(0, 1), (0, 2), (3, 2), (2, 4), (4, 5)]);
        let n_idx = node_index(0);
        let outgoing = {
            let mut n: Vec<_> = g.neighbors_directed(n_idx, Direction::Outgoing).collect();
            n.sort_unstable();
            n
        };
        assert_eq!(outgoing, vec![node_index(1), node_index(2)]);
    }

    #[test]
    fn test_can_find_orphan_nodes() {
        let mut g =
            DiGraph::<u8, ()>::from_edges(&[(0, 1), (0, 2), (3, 2), (2, 4), (4, 5), (7, 5)]);
        g.add_node(6);
        println!(
            "{:?}",
            dot::Dot::with_config(&g, &[dot::Config::NodeIndexLabel, dot::Config::EdgeNoLabel])
        );
        let orphan_nodes: Vec<NodeIndex<u32>> = find_orphan_nodes(&g);
        assert_eq!(orphan_nodes, vec![NodeIndex::new(6), NodeIndex::new(8)])
    }

    #[test]
    fn test_no_orphan_nodes_in_valid_graph() {
        let nodes = [0, 1, 2, 3, 4, 5, 6, 7];
        let edges = [(0, 1), (0, 2), (1, 3)];
        let graph = QueryGraph::new_from_ids_and_edges(nodes.to_vec(), edges.to_vec()).unwrap();
        let orphan_nodes: Vec<NodeIndex<u8>> = find_orphan_nodes(&graph.inner);
        assert_eq!(orphan_nodes, Vec::new())
    }

    #[test]
    fn test_can_generate_from_valid_edge_pairs() {
        let edges = [(0, 1), (0, 2), (3, 2), (2, 4), (4, 5), (7, 5)];
        assert!(ValidGraphData::new_from_edges(&edges).is_some())
    }

    #[test]
    fn test_does_not_generate_from_invalid_edge_pairs() {
        let edges = [];
        assert!(ValidGraphData::new_from_edges(&edges).is_none())
    }

    #[test]
    fn test_generate_from_valid_id_edges() {
        let nodes = [0, 1, 2, 3];
        let edges = [(0, 1), (0, 2), (1, 3)];
        assert!(ValidGraphData::new_from_id_edge_pairs(&nodes, &edges).is_some());
    }

    #[test]
    fn test_prevents_missing_node_information() {
        let nodes = [0, 1, 2];
        let edges = [(0, 1), (0, 2), (1, 3)];
        assert!(ValidGraphData::new_from_id_edge_pairs(&nodes, &edges).is_none());
    }

    #[test]
    fn test_successfully_filters_orphan_nodes() {
        let nodes = [0, 1, 2, 3, 4, 5, 6, 7];
        let edges = [(0, 1), (0, 2), (1, 3)];
        let data = ValidGraphData::new_from_id_edge_pairs(&nodes, &edges);
        let expected_nodes = [0, 1, 2, 3];
        assert_eq!(data.unwrap().nodes, expected_nodes);
    }

    #[test]
    fn test_graph_generates_on_valid_edge_data() {
        let edges = [(0, 1), (0, 2), (3, 2), (2, 4), (4, 5), (7, 5)];
        let graph = QueryGraph::new_from_edges(edges.to_vec());
        assert!(graph.is_some())
    }

    #[test]
    fn test_graph_generates_on_valid_edge_node_data() {
        let nodes = [0, 1, 2, 3, 4, 5, 7];
        let edges = [(0, 1), (0, 2), (3, 2), (2, 4), (4, 5), (7, 5)];
        let graph = QueryGraph::new_from_ids_and_edges(nodes.to_vec(), edges.to_vec());
        assert!(graph.is_some())
    }

    #[test]
    fn test_graph_does_not_generate_from_cyclic() {
        let nodes = [0, 1, 2, 3, 4, 5, 7];
        let edges = [(0, 1), (0, 2), (3, 2), (2, 4), (4, 5), (7, 5), (5, 0)];
        let graph = QueryGraph::new_from_ids_and_edges(nodes.to_vec(), edges.to_vec());
        assert!(graph.is_none())
    }

    #[test]
    fn test_graph_layout_correct() {
        let edges = [(0, 1), (0, 2), (3, 2), (2, 4), (4, 5), (7, 5)];
        let graph = QueryGraph::new_from_edges(edges.to_vec()).unwrap();
        println!(
            "{:?}",
            dot::Dot::with_config(
                &graph.inner,
                &[dot::Config::NodeIndexLabel, dot::Config::EdgeNoLabel]
            )
        );
    }

    #[test]
    fn test_externals_finds_src_nodes_in_valid_graph() {
        let nodes = [0, 1, 2, 3, 4, 5, 7];
        let edges = [(0, 1), (0, 2), (3, 2), (2, 4), (4, 5), (7, 5)];
        let graph = QueryGraph::new_from_ids_and_edges(nodes.to_vec(), edges.to_vec()).unwrap();
        let externals: Vec<_> = graph.inner.externals(Direction::Incoming).collect();
        println!(
            "{:?}",
            dot::Dot::with_config(
                &graph.inner,
                &[dot::Config::NodeIndexLabel, dot::Config::EdgeNoLabel]
            )
        );
        assert_eq!(
            externals,
            vec![
                graph.get_index(0).unwrap(),
                graph.get_index(3).unwrap(),
                graph.get_index(7).unwrap()
            ]
        );
        // This way, the specific index values are completely meaningless and be compressed at will
        // and we don't have to traverse the map to "find" the "weights" (ids)
    }

    #[test]
    fn test_node_labels_work_correctly() {
        let nodes = [31, 18, 9, 243, 11, 86, 109];
        let edges = [(31, 18), (31, 9), (243, 9), (9, 11), (11, 86), (109, 86)];
        let graph = QueryGraph::new_from_ids_and_edges(nodes.to_vec(), edges.to_vec()).unwrap();
        let output = format!(
            "{:?}",
            dot::Dot::with_config(&graph.inner, &[dot::Config::EdgeNoLabel])
        );
        assert_eq!(
            output,
            r#"digraph {
    0 [ label = "9" ]
    1 [ label = "11" ]
    2 [ label = "18" ]
    3 [ label = "31" ]
    4 [ label = "86" ]
    5 [ label = "109" ]
    6 [ label = "243" ]
    3 -> 2 [ ]
    3 -> 0 [ ]
    6 -> 0 [ ]
    0 -> 1 [ ]
    1 -> 4 [ ]
    5 -> 4 [ ]
}
"#
        )
    }

    #[test]
    fn test_correctly_finds_root_nodes_by_node_id() {
        let nodes = [31, 18, 9, 243, 11, 86, 109];
        let edges = [(31, 18), (31, 9), (243, 9), (9, 11), (11, 86), (109, 86)];
        let graph = QueryGraph::new_from_ids_and_edges(nodes.to_vec(), edges.to_vec()).unwrap();
        let root_nodes = graph.get_root_nodes();
        println!(
            "{:?}",
            dot::Dot::with_config(
                &graph.inner,
                &[dot::Config::NodeIndexLabel, dot::Config::EdgeNoLabel]
            )
        );

        assert_eq!(root_nodes, vec![31, 109, 243]);
        // This way, the specific index values are completely meaningless and be compressed at will
        // and we don't have to traverse the map to "find" the "weights" (ids)
    }
}
