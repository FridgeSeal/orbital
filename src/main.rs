mod query_graph;
mod settings;

fn main() {
    println!("Hello, world!");
    query_graph::query::QueryCollection::new();
}

/*
Startup.
Read configuration.
Parse + validate configuration.
Find SQL queries.
Parse SQL queries (including folding in any provided values).
Validate SQL queries against source database.
Build query dependency tree.
Enrich query dependency tree with freshness state.
iterate through all sources {
    check change-state (freshness, table-hash, etc)
    run any freshness or schedule induced tasks
    bump any downstream tasks to run
}

oh my god they're not even running this...automatically? They're just running
the fucking command line tool via a scheduled job. Wild.
*/
