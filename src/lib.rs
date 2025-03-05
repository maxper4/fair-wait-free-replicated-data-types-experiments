mod dag;
pub mod crdt;
mod process;
mod rendering;
mod config;
mod network;

use std::cmp::Ordering;
use std::vec;

use crdt::{Operation, OperationParameter, VertexLabel, CRDT};
use crdt::reconciliation_functions::{basic_exploration, fair_reconciliation_no_n};
use crdt::legal_functions::total;
use dag::{Dag, Vertex, VertexId};

use config::Config;

pub async fn run() {
    let config = Config::get("config.toml");
    println!("{:?}", config.peers[0].ip);
    let (network_chan, network_task) = network::run(config).await;
    tokio::join!(network_task);

    fn mutate_counter(state: &i32, op: &Operation<()>) -> i32 {
        state + 1
    }
    
    let counter = CRDT::new(0, mutate_counter, basic_exploration, total);
    let n = 4;
    // let (processes_to_network_sender, mut processes_to_network_receiver): (Sender<CRDTOperationMessage>, Receiver<CRDTOperationMessage>) = tokio::sync::mpsc::channel(100);
    // let mut network_to_processes_senders = vec![];
    // let mut threads = vec![];
    // let mut executors = vec![];
    // for i in 0..n {
    //     let (network_to_process_sender, network_to_process_receiver): (Sender<CRDTOperationMessage>, Receiver<CRDTOperationMessage>) = tokio::sync::mpsc::channel(100);
    //     network_to_processes_senders.push(network_to_process_sender);
    //     let mut process = Process::new(i, &counter, network_to_process_receiver, &processes_to_network_sender);
    //     let executor = process.execute_chan_sender.clone();
    //     let handle = tokio::spawn(async move {
    //         process.run().await;
    //     });
    //     executors.push(executor);
    //     threads.push(handle);
    // }

    // let network_task = tokio::spawn(async move {
    //     // networking
    //     while let Some(v) = processes_to_network_receiver.recv().await {
    //         for i in 0..n {
    //             let sender = network_to_processes_senders[i as usize].clone();
    //             let v = v.clone();
    //             tokio::spawn(async move {
    //                 tokio::time::sleep(std::time::Duration::from_millis((100 * i).into())).await;   // simulate random network delay
    //                 sender.send(v.clone()).await.expect("oops! the network sender panicked");
    //             });
    //         }
    //     }
    // });

    // let executing_task = tokio::spawn(async move {
    //     for i in 0..n {
    //         tokio::time::sleep(std::time::Duration::from_millis((100 * i).into())).await;   // simulate random processor speed
    //         executors[i as usize].send(Operation::new(0)).await;
    //     }
    // });

    // tokio::join!(network_task, executing_task);
}