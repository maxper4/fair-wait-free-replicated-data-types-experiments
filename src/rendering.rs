use std::fs;
use std::io::Write;

use graphviz_rust::cmd::Format;
use graphviz_rust::exec;
use graphviz_rust::dot_structures::{Graph, Id, Stmt, Node, Edge, NodeId, EdgeTy, Vertex, Attribute};
use graphviz_rust::printer::PrinterContext;
use graphviz_rust::dot_generator::*;

use crate::dag::Dag;


fn to_graph_viz<T>(d: &Dag<T>) -> graphviz_rust::dot_structures::Graph {
    let mut g = graph!(di id!("id"));
    let mut toexplore = vec![d.get_root()];
    let mut explored = vec![];
    while toexplore.len() > 0 {
        let head = toexplore.pop().unwrap();
        g.add_stmt(stmt!(node!(head.id; attr!("label", head.id.to_string()))));
        let children = d.get_edges_to_vertex(head.id);
        for c in children {
            if explored.contains(&head.id) {
                continue;
            }
            g.add_stmt(stmt!(edge!(node_id!(head.id) => node_id!(c.id), vec![attr!("a","b")])));
            toexplore.push(c);
        }
        explored.push(head.id);
    }
    
    g
}

pub fn print_graph<T>(d: &Dag<T>, file_name: String) {
    let res = exec(to_graph_viz(d), &mut PrinterContext::default(), vec![Format::Png.into()]);
    match res {
        Ok(e) => {     
            let mut file = fs::OpenOptions::new();
            file.write(true);
            file.create(true);
            let mut file = file.open(format!("display/{file_name}")).unwrap();
            file.write_all(&e).unwrap();
        }
        Err(e) =>{ println!("Error: {:?}", e); }
    }
}