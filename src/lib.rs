mod dag;
pub mod crdt;
mod process;
mod rendering;

use std::cmp::Ordering;
use std::vec;

use crdt::{Operation, OperationParameter, VertexLabel, CRDT};
use crdt::reconciliation_functions::{basic_exploration, fair_reconciliation_no_n};
use crdt::legal_functions::total;
use dag::{Dag, Vertex, VertexId};

use tokio::sync::mpsc::{Sender, Receiver};

use crate::process::{CRDTOperationMessage, Process};

pub async fn run() {
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct CounterParameter {
        inc: u32,
    }

    impl Default for CounterParameter {
        fn default() -> Self {
            CounterParameter {
                inc: 1,
            }
        }
    }

    impl OperationParameter for CounterParameter {}

    fn mutate_counter(state: &u32, op: &Operation<CounterParameter>) -> u32 {
        *state + op.params.inc
    }
    
   let mut counter = CRDT::new(0, mutate_counter, basic_exploration, total);

    let n = 4;
    let (processes_to_network_sender, mut processes_to_network_receiver): (Sender<CRDTOperationMessage<CounterParameter>>, Receiver<CRDTOperationMessage<CounterParameter>>) = tokio::sync::mpsc::channel(100);
    let mut network_to_processes_senders = vec![];
    let mut threads = vec![];
    let mut executors = vec![];
    for i in 0..n {
        let (network_to_process_sender, network_to_process_receiver): (Sender<CRDTOperationMessage<CounterParameter>>, Receiver<CRDTOperationMessage<CounterParameter>>) = tokio::sync::mpsc::channel(100);
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
            executors[i as usize].send(Operation::new(0, CounterParameter { inc: i })).await;
        }
    });

    tokio::join!(network_task, executing_task);
}