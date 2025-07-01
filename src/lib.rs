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
    struct Transaction {}

    impl Default for Transaction {
        fn default() -> Self {
            Transaction {}
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
        match op.params {
            MempoolParameter::Add(ref tx) => {
                state.push(tx.clone());
            },
            MempoolParameter::Prune() => {
                let mut result = vec![];
                for _ in 0..BLOCK_SIZE {
                    result.push(state.remove(0));
                }
                println!("New block: {:?}", result);
            }
        }
        state
    }

    mutate_if_legal!(Vec<Transaction>, MempoolParameter, mutate, mutate_mempool, mempool_legality);

    let mempool = CRDT::new(vec![], mutate, basic_exploration, total);

    let mut process = Process::new(config.id, &mempool, from_network_chan, &to_network_chan);
    let process_executor = process.execute_chan_sender.clone();

    let process_task = tokio::spawn(async move {
                process.run().await;
            });

    let execute_task = tokio::spawn(async move {
        if config.id == 1 {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                let op = Operation::new(0, MempoolParameter::Add(Transaction::default()));
                process_executor.send(op).await.unwrap();
            }
        }
        if config.id == 2 {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                let op = Operation::new(0, MempoolParameter::Prune());
                process_executor.send(op).await.unwrap();
            }
        }
        
    });

    // TODO: here execute ops
    tokio::join!(network_task, process_task, execute_task);
}