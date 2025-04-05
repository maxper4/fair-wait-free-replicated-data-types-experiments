use crate::crdt::{Operation, OperationParameter, VertexLabel, CRDT};
use crate::dag::{Dag, Vertex, VertexId};

use std::vec::IntoIter;
use std::collections::HashMap;

pub fn basic_exploration<P>(dag: &Dag<VertexLabel<P>>) -> IntoIter<VertexLabel<P>>  // works when there is no conflict
where P: OperationParameter {
    let mut toexplore = vec![VertexId::new(0, 0)];
    let mut order: Vec<VertexLabel<P>> = vec![];
    let mut children = vec![];

    while toexplore.len() > 0 {
        let head = toexplore.pop().unwrap();
        let v: &Vertex<_> = dag.get_vertex(head).unwrap();
        order.push(v.label.clone());
        for c in dag.get_edges_to_vertex(head) {
            if !children.contains(&c.id) {
                children.push(c.id);
            }
        }
        if toexplore.len() == 0 {
            toexplore.extend(children.clone());
            children.clear();
        }
    }
    order.into_iter()
}

pub fn handling_conflict<P>(op_order: Vec<Vec<Option<usize>>>) -> impl Fn(&Dag<VertexLabel<P>>) -> IntoIter<VertexLabel<P>>
 where P: OperationParameter { 
    move |dag: &Dag<VertexLabel<_>>| {    // works when there is conflict, returns a function given an order
    let mut toexplore = vec![VertexId::new(0, 0)];
    let mut order: Vec<VertexLabel<_>> = vec![];
    let mut children = vec![];

    while toexplore.len() > 0 {
        let head = toexplore.pop().unwrap();
        for c in dag.get_edges_to_vertex(head) {
            if !children.contains(&c.id) {
                children.push(c.id);
            }
        }

        if toexplore.len() == 0 {   // we explored all vertices at the same level, children is now all vertices at distance k+1
            toexplore.extend(children.clone());
            let mut next_children: Vec<&Vertex<VertexLabel<_>>> = children.clone().into_iter().map(|c| dag.get_vertex(c).unwrap()).collect::<Vec<_>>();
            children.clear();

            if next_children.len() > 1 {
                next_children.sort_by(|x: &&Vertex<VertexLabel<_>>, y: &&Vertex<VertexLabel<_>> | x.label.op.id.cmp(&y.label.op.id));  // what sort should we use?
                
                while next_children.len() > 1 {    // take concurrent operations 2 by 2 and check conflicts
                    let v1: &Vertex<VertexLabel<_>> = next_children.pop().unwrap();
                    let mut conflicted = false;
                    let mut toremove = vec![];

                    for v2 in next_children.iter() {
                        let winner = op_order[v1.label.op.id][v2.label.op.id];
                        match winner {
                            Some(label) => { 
                                if label == v1.label.op.id { 
                                    toremove.push(*v2);
                                }
                                else { 
                                    conflicted = true; 
                                    break; 
                                }
                            }
                            None => { continue; }   // commutes
                        }
                    }
                    
                    // clean loosers
                    for v in toremove.into_iter() {
                        next_children.retain(|x: &&Vertex<VertexLabel<_>>| x.id != v.id);
                    }

                    if !conflicted {    // if no conflict with the whole set of concurrent operations, add in the order
                        order.push(v1.label.clone());
                    }
                }
            } 
            if next_children.len() == 1 {
                order.push(next_children.pop().unwrap().label.clone());
            }
        }
    }
    order.into_iter()
}}

pub fn fair_reconciliation<P>(op_conflicts: Vec<Vec<bool>>) -> impl Fn(&Dag<VertexLabel<P>>) -> IntoIter<VertexLabel<P>>
where P: OperationParameter { 
    move |dag: &Dag<VertexLabel<_>>| {    // reconciliation is based on process fairness rather than semantically
    let mut toexplore = vec![VertexId::new(0, 0)];
    let mut order: Vec<VertexLabel<_>> = vec![];
    let mut children = vec![];
    let mut scores:HashMap<u32, u32> = HashMap::new();

    while toexplore.len() > 0 {
        let head = toexplore.pop().unwrap();
        for c in dag.get_edges_to_vertex(head) {
            if !children.contains(&c.id) {
                children.push(c.id);
            }
        }
        if toexplore.len() == 0 {   // we explored all vertices at the same level, children is now all vertices at distance k+1
            toexplore.extend(children.clone());
            let mut next_children: Vec<&Vertex<VertexLabel<_>>>  = children.clone().into_iter().map(|c| dag.get_vertex(c).unwrap()).collect::<Vec<_>>();
            children.clear();

            if next_children.len() > 1 {
                next_children.sort_by(|x: &&Vertex<VertexLabel<_>>, y: &&Vertex<VertexLabel<_>> | x.label.op.id.cmp(&y.label.op.id));  // what sort should we use?

                let mut winners: Vec<&Vertex<VertexLabel<_>>>  = vec![];
                let mut loosers: Vec<&Vertex<VertexLabel<_>>> = vec![];
                
                while next_children.len() > 1 {    // take concurrent operations 2 by 2 and check conflicts
                    let v1: &Vertex<VertexLabel<_>> = next_children.pop().unwrap();
                    let mut conflicted = false;
                    let mut toremove = vec![];

                    for v2 in next_children.iter() {
                        let conflict = op_conflicts[v1.label.op.id][v2.label.op.id];
                        if conflict {
                            let p1 = v1.label.process_id;
                            let p2 = v2.label.process_id;
                            let score_p1 = *scores.get(&p1).unwrap_or(&0);
                            let score_p2 = *scores.get(&p2).unwrap_or(&0);

                            if score_p1 > score_p2 || (score_p1 == score_p2 && p1 >= p2) {
                                toremove.push(*v2);
                            } else {
                                conflicted = true;
                            }
                        }
                    }
                    
                    // clean loosers
                    for v in toremove.into_iter() {
                        next_children.retain(|x: &&Vertex<VertexLabel<_>>| x.id != v.id);
                        loosers.push(v);
                    }

                    if conflicted {
                        loosers.push(v1);
                    } else {    // if no conflict with the whole set of concurrent operations, add in the order
                        winners.push(v1);
                    }
                }
                if next_children.len() == 1 {
                    let v: &Vertex<VertexLabel<_>> = next_children.pop().unwrap();
                    winners.push(v);
                }

                for v in winners {
                    scores.insert(v.label.process_id, 0);
                    order.push(v.label.clone());
                }

                for v in loosers {
                    let old_score = *scores.get(&v.label.process_id).unwrap_or(&0);
                    scores.insert(v.label.process_id, old_score + 1);
                }
            }
            if next_children.len() == 1 {
                let v: &Vertex<VertexLabel<_>> = next_children.pop().unwrap();
                order.push(v.label.clone());
                scores.insert(v.label.process_id, 0);
            }
        }
    }
    order.into_iter()
}}