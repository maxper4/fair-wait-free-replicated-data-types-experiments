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
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

use crate::process::{CRDTOperationMessage, Process};

pub async fn run() {
    let config = Config::get("config.toml");
    println!("Process {} launched.", config.id);

    let (to_network_chan, from_network_chan, network_task) = network::run(&config).await;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Transaction {
        id: usize
    }

    impl Transaction {
        pub fn new(id: usize) -> Self {
            Transaction { id }
        }
    }

    impl Default for Transaction {
        fn default() -> Self {
            Transaction { id: 0}
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    enum MempoolParameter {
        Add(Transaction),
        Prune(),
    }

    impl Default for MempoolParameter {
        fn default() -> Self {
            MempoolParameter::Add(Transaction::default())
        }
    }

    impl OperationParameter for MempoolParameter {}

    const BLOCK_SIZE :u32 = 10;

    fn mempool_legality(state: &Vec<Transaction>, op: &Operation<MempoolParameter>) -> bool {
        match op.id {
            0 => {
                match op.params {
                    MempoolParameter::Add(ref tx) => {
                        !state.contains(tx)
                    },
                    _ => {
                        false
                    }
                }
            },
            1 => {
                match op.params {
                    MempoolParameter::Prune() => {
                        state.len() >= BLOCK_SIZE as usize
                    },
                    _ => {
                        false
                    }
                }
            },
            _ => {
                false
            }
        }
    }

    fn mutate_mempool(state: &Vec<Transaction>, op: &Operation<MempoolParameter>) -> Vec<Transaction> {
        let mut state = state.clone();
        match op.id {
            0 => {
                match op.params {
                    MempoolParameter::Add(ref tx) => {
                        state.push(tx.clone());
                    },
                    _ => {}
                }
            },
            1 => {
                match op.params {
                    MempoolParameter::Prune() => {
                        let mut result = vec![];
                        for _ in 0..BLOCK_SIZE {
                            result.push(state.remove(0));
                        }
                        println!("New block: {:?}", result);
                    },
                    _ => {}
                }
            },
            _ => {}
        }
        
        state
    }

    fn mempool_order(v1: &Vertex<VertexLabel<MempoolParameter>>, v2: &Vertex<VertexLabel<MempoolParameter>>) -> Ordering {
        match (v1.label.op.id, v2.label.op.id) {
            (0, 1) => Ordering::Less,  // add before prune
            (1, 0) => Ordering::Greater, // prune after add
            _ => v1.id.process_id.cmp(&v2.id.process_id) // same operation id
        }
    }

    order_based_reconciliation!(Vec<Transaction>, MempoolParameter, mempool_order, mempool_reconciliation);

    mutate_if_legal!(Vec<Transaction>, MempoolParameter, mutate, mutate_mempool, mempool_legality);

    let mempool = CRDT::new(vec![], mutate, mempool_reconciliation, total);

    let mut process = Process::new(config.id, &mempool, from_network_chan, &to_network_chan);
    let process_executor = process.execute_chan_sender.clone();

    let process_task = tokio::spawn(async move {
                process.run().await;
            });

    let execute_task = tokio::spawn(async move {
        if config.id != 2 {
            let mut id = config.id as usize;
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                let op = Operation::new(0, MempoolParameter::Add(Transaction::new(id)));
                id += config.id as usize;
                process_executor.send(op).await.unwrap();
            }
        }
        if config.id == 2 {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
                let op = Operation::new(1, MempoolParameter::Prune());
                process_executor.send(op).await.unwrap();
            }
        }
        
    });

    // TODO: here execute ops
    tokio::join!(network_task, process_task, execute_task);
}