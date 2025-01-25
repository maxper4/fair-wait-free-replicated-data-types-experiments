mod dag;
mod crdt;
mod process;

use std::vec;

use crdt::CRDT;
use dag::Dag;
use process::Process;

use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use std::thread;

fn main() {

    let basic_exploration = |dag: &Dag<usize>| {    // works when there is no conflict
        let mut toexplore = vec![dag.get_root()];
        let mut order = vec![];
        while toexplore.len() > 0 {
            let head = toexplore.pop().unwrap();
            order.push(head.label);
            toexplore.extend(dag.get_edges_to_vertex(head.id as usize).into_iter());
        }
        order.into_iter()
    };

    let handling_conflict = |op_order: Vec<Vec<Option<usize>>>| { move |dag: &Dag<usize>| {    // works when there is conflict, returns a function given an order
        let mut toexplore = vec![dag.get_root()];
        let mut order = vec![];
        while toexplore.len() > 0 {
            let head = toexplore.pop().unwrap();
            order.push(head.label);
            let children = dag.get_edges_to_vertex(head.id as usize);
            if children.len() > 1 {
                let mut children = children.into_iter().collect::<Vec<_>>();
                children.sort_by(|x, y| x.label.cmp(&y.label));  // what sort should we use?
                
                while children.len() > 1 {    // take concurrent operations 2 by 2 and check conflicts
                    let v1 = children.pop().unwrap();
                    let mut conflicted = false;
                    let mut toremove = vec![];

                    for v2 in children.iter() {
                        let winner = op_order[v1.label][v2.label];
                        match winner {
                            Some(label) => { 
                                if label == v1.label { 
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

                    if !conflicted {
                        toexplore.push(v1);
                    }
                    
                }
            } else {
                toexplore.extend(children.into_iter());
            }
        }
        order.into_iter()
    }};

    let mut counter = CRDT::new(0, vec![|x| x + 1], basic_exploration);

    counter.apply(0);
    counter.apply(0);
    counter.apply(0);
    
    let result = counter.read();
    let seq = basic_exploration(&counter.dag).collect::<Vec<usize>>();
    println!("Counter {:?} = {}", seq, result);

    let add = |mut x: Vec<i32>| { x.push(4); x };
    let remove = |mut x: Vec<i32>| { x.pop(); x };

    let mut set = CRDT::new(vec![1, 2, 3], vec![add, remove], basic_exploration);
    set.apply(1);
    set.apply(1);
    set.apply(0);

    let result = set.read();
    let seq = basic_exploration(&set.dag).collect::<Vec<usize>>();
    println!("Set {:?} = {:?}", seq, result);

    
    // remove wins (1)
    let add_remove_order = vec![
        vec![None, Some(1)],
        vec![Some(1), None]
    ];
    let add_remove_reconciliation = handling_conflict(add_remove_order);
    // adding concurrency for debugging
    let mut concurrent_set_dag = Dag::new(0);
    concurrent_set_dag.add_vertex(vec![0], 0);  // no concurrent, 0 stays
    concurrent_set_dag.add_vertex(vec![1], 1);
    concurrent_set_dag.add_vertex(vec![1], 0);  // concurrent, 1 wins
    concurrent_set_dag.add_vertex(vec![2, 3], 0);
    concurrent_set_dag.add_vertex(vec![2, 3], 1);
    concurrent_set_dag.add_vertex(vec![2, 3], 0); 
    concurrent_set_dag.add_vertex(vec![2, 3], 1);   // 4 concurrent, [1, 1] wins
    
    let seq = add_remove_reconciliation(&concurrent_set_dag).collect::<Vec<usize>>();
    println!("Concurrent Set {:?}", seq);

    let n = 4;
    let (processes_to_network_sender, processes_to_network_receiver): (Sender<usize>, Receiver<usize>) = mpsc::channel();
    let mut network_to_processes_senders = vec![];
    let mut threads = vec![];
    for i in 0..n {
        let (network_to_process_sender, network_to_process_receiver): (Sender<usize>, Receiver<usize>) = mpsc::channel();
        network_to_processes_senders.push(network_to_process_sender);
        let mut process = Process::new(i, &counter, network_to_process_receiver, &processes_to_network_sender);
        let t = thread::spawn(move || {
            process.run();
        });

        threads.push(t);
    }

    // networking
    for msg in processes_to_network_receiver {  // TODO: add some asynchrony by hand?
        for sender in network_to_processes_senders.iter() {
            sender.send(msg).expect("oops! the network sender panicked");
        }
    }

    for t in threads {
        t.join().expect("oops! the child thread panicked");
    }
}