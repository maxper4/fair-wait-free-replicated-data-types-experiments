mod dag;
mod crdt;
mod process;
mod rendering;

use std::collections::HashMap;
use std::vec;

use crdt::{Operation, VertexLabel, CRDT};
use dag::{Dag, Vertex, VertexId};
use process::{CRDTOperationMessage, Process};

use rendering::print_graph;
use tokio::sync::mpsc::{Sender, Receiver};

#[tokio::main]
async fn main() {

    let basic_exploration = |dag: &Dag<VertexLabel>| {    // works when there is no conflict
        let mut toexplore = vec![VertexId::new(0, 0)];
        let mut order = vec![];
        let mut children = vec![];

        while toexplore.len() > 0 {
            let head = toexplore.pop().unwrap();
            let v = dag.get_vertex(head).unwrap();
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
    };

    let handling_conflict = |op_order: Vec<Vec<Option<usize>>>| { move |dag: &Dag<VertexLabel>| {    // works when there is conflict, returns a function given an order
        let mut toexplore = vec![VertexId::new(0, 0)];
        let mut order = vec![];
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
                let mut next_children = children.clone().into_iter().map(|c| dag.get_vertex(c).unwrap()).collect::<Vec<_>>();
                children.clear();

                if next_children.len() > 1 {
                    next_children.sort_by(|x, y| x.label.op.id.cmp(&y.label.op.id));  // what sort should we use?
                    
                    while next_children.len() > 1 {    // take concurrent operations 2 by 2 and check conflicts
                        let v1 = next_children.pop().unwrap();
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
                            next_children.retain(|x| x.id != v.id);
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
    }};

    let fair_reconciliation = |op_conflicts: Vec<Vec<bool>>| { move |dag: &Dag<VertexLabel>| {    // reconciliation is based on process fairness rather than semantically
        let mut toexplore = vec![VertexId::new(0, 0)];
        let mut order = vec![];
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
                let mut next_children = children.clone().into_iter().map(|c| dag.get_vertex(c).unwrap()).collect::<Vec<_>>();
                children.clear();

                if next_children.len() > 1 {
                    next_children.sort_by(|x, y| x.label.op.id.cmp(&y.label.op.id));  // what sort should we use?
    
                    let mut winners = vec![];
                    let mut loosers = vec![];
                    
                    while next_children.len() > 1 {    // take concurrent operations 2 by 2 and check conflicts
                        let v1 = next_children.pop().unwrap();
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
                            next_children.retain(|x| x.id != v.id);
                            loosers.push(v);
                        }
    
                        if conflicted {
                            loosers.push(v1);
                        } else {    // if no conflict with the whole set of concurrent operations, add in the order
                            winners.push(v1);
                        }
                    }
                    if next_children.len() == 1 {
                        let v = next_children.pop().unwrap();
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
                    let v = next_children.pop().unwrap();
                    order.push(v.label.clone());
                    scores.insert(v.label.process_id, 0);
                }
            }
        }
        order.into_iter()
    }};

    let mut counter = CRDT::new(0, vec![|x| x + 1], basic_exploration);

    counter.apply(Operation::new(0), 0);
    counter.apply(Operation::new(0), 0);
    counter.apply(Operation::new(0), 0);

    let result = counter.read();
    let seq = basic_exploration(&counter.dag).map(|x| x.op.id).collect::<Vec<usize>>();
    println!("Counter {:?} = {}", seq, result);

    let add = |mut x: Vec<i32>| { x.push(4); x };
    let remove = |mut x: Vec<i32>| { x.pop(); x };

    let mut set = CRDT::new(vec![1, 2, 3], vec![add, remove], basic_exploration);
    set.apply(Operation::new(1), 0);
    set.apply(Operation::new(1), 0);
    set.apply(Operation::new(0), 0);

    let result = set.read();
    let seq = basic_exploration(&set.dag).map(|x| x.op.id).collect::<Vec<usize>>();
    println!("Set {:?} = {:?}", seq, result);

    
    // remove wins (1)
    let add_remove_order = vec![
        vec![None, Some(1)],
        vec![Some(1), None]
    ];
    let add_remove_reconciliation = handling_conflict(add_remove_order);
    // adding concurrency for debugging
    let mut concurrent_set_dag = Dag::new(VertexLabel::new(0, 0));
    concurrent_set_dag.add_vertex(vec![], Vertex::new(VertexId::new(1, 0), VertexLabel::new(0, 0)));  // no concurrent, 0 stays
    concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(2, 0), VertexLabel::new(1, 0)));
    concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(3, 0), VertexLabel::new(0, 0)));  // concurrent, 1 wins
    concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(4, 0), VertexLabel::new(0, 0)));
    concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(5, 0), VertexLabel::new(1, 0)));
    concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(6, 0), VertexLabel::new(0, 0))); 
    concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(7, 0), VertexLabel::new(1, 0)));   // 4 concurrent, [1, 1] wins
    
    let seq = add_remove_reconciliation(&concurrent_set_dag).map(|x| x.op.id).collect::<Vec<usize>>();
    println!("Concurrent Set {:?}", seq);

    let onlyconflict = vec![
        vec![true, true],
        vec![true, true]
    ];
    // adding concurrency for debugging
    let mut fair_concurrent_set_dag = Dag::new(VertexLabel::new(0, 0));
    fair_concurrent_set_dag.add_vertex(vec![], Vertex::new(VertexId::new(1, 0), VertexLabel::new(0, 1)));  // no concurrent, 0 stays
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(2, 0), VertexLabel::new(1, 2)));
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(3, 0), VertexLabel::new(0, 1)));  // concurrent, 1 wins (id higher)
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(4, 0), VertexLabel::new(1, 2))); //p2 is rollbacked => score of 1
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(5, 0), VertexLabel::new(0, 1))); // concurrent, 0 wins (score higher)
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(4, 0), VertexId::new(5, 0)], Vertex::new(VertexId::new(6, 0), VertexLabel::new(1, 2)));
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(4, 0), VertexId::new(5, 0)], Vertex::new(VertexId::new(7, 0), VertexLabel::new(0, 1))); 
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(4, 0), VertexId::new(5, 0)], Vertex::new(VertexId::new(8, 0), VertexLabel::new(0, 3))); // 3 concurrent, 1 (p2) wins (score higher)  (p1:1, p3:1) 
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(6, 0), VertexId::new(7, 0), VertexId::new(8, 0)], Vertex::new(VertexId::new(9, 0), VertexLabel::new(0, 2)));
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(6, 0), VertexId::new(7, 0), VertexId::new(8, 0)], Vertex::new(VertexId::new(10, 0), VertexLabel::new(0, 1))); 
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(6, 0), VertexId::new(7, 0), VertexId::new(8, 0)], Vertex::new(VertexId::new(11, 0), VertexLabel::new(1, 3))); // 3 concurrent, 1 (p3) wins (p1: 2, p2: 1, p3:0)
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(9, 0), VertexId::new(10, 0), VertexId::new(11, 0)], Vertex::new(VertexId::new(12, 0), VertexLabel::new(0, 2)));
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(9, 0), VertexId::new(10, 0), VertexId::new(11, 0)], Vertex::new(VertexId::new(13, 0), VertexLabel::new(1, 1))); 
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(9, 0), VertexId::new(10, 0), VertexId::new(11, 0)], Vertex::new(VertexId::new(14, 0), VertexLabel::new(0, 3))); // 3 concurrent, 1 (p1) wins (p1: 0, p2: 2, p3: 1)
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(12, 0), VertexId::new(13, 0), VertexId::new(14, 0)], Vertex::new(VertexId::new(15, 0), VertexLabel::new(1, 2)));
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(12, 0), VertexId::new(13, 0), VertexId::new(14, 0)], Vertex::new(VertexId::new(16, 0), VertexLabel::new(0, 1))); 
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(12, 0), VertexId::new(13, 0), VertexId::new(14, 0)], Vertex::new(VertexId::new(17, 0), VertexLabel::new(0, 3))); // 3 concurrent, 1 (p2) wins
    let add_remove_fair_reconciliation = fair_reconciliation(onlyconflict);
    let seq = add_remove_fair_reconciliation(&fair_concurrent_set_dag).map(|x| x.op.id).collect::<Vec<usize>>();
    println!("Fair concurrent Set {:?}", seq);  // should be [0, 1, 0, 1, 1, 1, 1]

    let counter = CRDT::new(0, vec![|x| x + 1], basic_exploration);
    let n = 4;
    let (processes_to_network_sender, mut processes_to_network_receiver): (Sender<CRDTOperationMessage>, Receiver<CRDTOperationMessage>) = tokio::sync::mpsc::channel(100);
    let mut network_to_processes_senders = vec![];
    let mut threads = vec![];
    let mut executors = vec![];
    for i in 0..n {
        let (network_to_process_sender, network_to_process_receiver): (Sender<CRDTOperationMessage>, Receiver<CRDTOperationMessage>) = tokio::sync::mpsc::channel(100);
        network_to_processes_senders.push(network_to_process_sender);
        let mut process = Process::new(i, &counter, network_to_process_receiver, &processes_to_network_sender);
        let executor = process.execute_chan_sender.clone();
        let handle = tokio::spawn(async move {
            process.run().await;
        });
        executors.push(executor);
        threads.push(handle);
    }

    let network_task = tokio::spawn(async move {
        // networking
        while let Some(v) = processes_to_network_receiver.recv().await {
            for i in 0..n {
                let sender = network_to_processes_senders[i as usize].clone();
                let v = v.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis((100 * i).into())).await;   // simulate random network delay
                    sender.send(v.clone()).await.expect("oops! the network sender panicked");
                });
            }
        }
    });

    let executing_task = tokio::spawn(async move {
        for i in 0..n {
            tokio::time::sleep(std::time::Duration::from_millis((100 * i).into())).await;   // simulate random processor speed
            executors[i as usize].send(Operation::new(0)).await;
        }
    });

    tokio::join!(network_task, executing_task);
}