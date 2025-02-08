mod dag;
mod crdt;
mod process;

use std::vec;

use crdt::{Operation, VertexLabel, CRDT};
use dag::Dag;
use process::Process;

use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::mpsc;
use std::thread;

#[tokio::main]
async fn main() {

    let basic_exploration = |dag: &Dag<VertexLabel>| {    // works when there is no conflict
        let mut toexplore = vec![dag.get_root()];
        let mut order = vec![];
        while toexplore.len() > 0 {
            let head = toexplore.pop().unwrap();
            order.push(head.label.clone());
            toexplore.extend(dag.get_edges_to_vertex(head.id as usize).into_iter());
        }
        order.into_iter()
    };

    let handling_conflict = |op_order: Vec<Vec<Option<usize>>>| { move |dag: &Dag<VertexLabel>| {    // works when there is conflict, returns a function given an order
        let mut toexplore = vec![dag.get_root()];
        let mut order = vec![];
        while toexplore.len() > 0 {
            let head = toexplore.pop().unwrap();
            order.push(head.label.clone());
            let children = dag.get_edges_to_vertex(head.id as usize);
            if children.len() > 1 {
                let mut children = children.into_iter().collect::<Vec<_>>();
                children.sort_by(|x, y| x.label.op.id.cmp(&y.label.op.id));  // what sort should we use?
                
                while children.len() > 1 {    // take concurrent operations 2 by 2 and check conflicts
                    let v1 = children.pop().unwrap();
                    let mut conflicted = false;
                    let mut toremove = vec![];

                    for v2 in children.iter() {
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
                        children.retain(|x| x.id != v.id);
                    }

                    if !conflicted {    // if no conflict with the whole set of concurrent operations, add in the order
                        toexplore.push(v1);
                    }
                }
                if children.len() == 1 {
                    toexplore.push(children.pop().unwrap());
                }
            } else {
                toexplore.extend(children.into_iter());
            }
        }
        order.into_iter()
    }};

    let mut counter = CRDT::new(0, vec![|x| x + 1], basic_exploration);

    counter.apply(VertexLabel::new(0, 0));
    counter.apply(VertexLabel::new(0, 0));
    counter.apply(VertexLabel::new(0, 0));
    
    let result = counter.read();
    let seq = basic_exploration(&counter.dag).map(|x| x.op.id).collect::<Vec<usize>>();
    println!("Counter {:?} = {}", seq, result);

    let add = |mut x: Vec<i32>| { x.push(4); x };
    let remove = |mut x: Vec<i32>| { x.pop(); x };

    let mut set = CRDT::new(vec![1, 2, 3], vec![add, remove], basic_exploration);
    set.apply(VertexLabel::new(1, 0));
    set.apply(VertexLabel::new(1, 0));
    set.apply(VertexLabel::new(0, 0));

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
    concurrent_set_dag.add_vertex(vec![0], VertexLabel::new(0, 0));  // no concurrent, 0 stays
    concurrent_set_dag.add_vertex(vec![1], VertexLabel::new(1, 0));
    concurrent_set_dag.add_vertex(vec![1], VertexLabel::new(0, 0));  // concurrent, 1 wins
    concurrent_set_dag.add_vertex(vec![2, 3], VertexLabel::new(0, 0));
    concurrent_set_dag.add_vertex(vec![2, 3], VertexLabel::new(1, 0));
    concurrent_set_dag.add_vertex(vec![2, 3], VertexLabel::new(0, 0)); 
    concurrent_set_dag.add_vertex(vec![2, 3], VertexLabel::new(1, 0));   // 4 concurrent, [1, 1] wins
    
    let seq = add_remove_reconciliation(&concurrent_set_dag).map(|x| x.op.id).collect::<Vec<usize>>();
    println!("Concurrent Set {:?}", seq);

    let n = 4;
    let (processes_to_network_sender, mut processes_to_network_receiver): (Sender<VertexLabel>, Receiver<VertexLabel>) = tokio::sync::mpsc::channel(100);
    let mut network_to_processes_senders = vec![];
    let mut threads = vec![];
    for i in 0..n {
        let (network_to_process_sender, network_to_process_receiver): (Sender<VertexLabel>, Receiver<VertexLabel>) = tokio::sync::mpsc::channel(100);
        network_to_processes_senders.push(network_to_process_sender);
        let mut process = Process::new(i, &counter, network_to_process_receiver, &processes_to_network_sender);
        let executor = process.execute_chan_sender.clone();
        let handle = tokio::spawn(async move {
            process.run().await;
        });
        executor.send(Operation::new(0)).await;
        threads.push(handle);
    }
    
    // networking
    while let Some(v) = processes_to_network_receiver.recv().await {
        for sender in network_to_processes_senders.iter() {
            sender.send(v.clone()).await.expect("oops! the network sender panicked");
        }
    }
  
  
}
