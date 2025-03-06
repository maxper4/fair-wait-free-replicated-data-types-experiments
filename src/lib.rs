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
use tokio::sync::mpsc::Sender;

use crate::process::{CRDTOperationMessage, Process};

pub async fn run() {
    let config = Config::get("config.toml");
    let (to_network_chan, from_network_chan, network_task) = network::run(&config).await;

    fn mutate_counter(state: &u32, op: &Operation<()>) -> u32 {
        *state + 1
    }

    let counter = CRDT::new(0, mutate_counter, basic_exploration, total);
    let mut process = Process::new(config.id, &counter, from_network_chan, &to_network_chan);
    let process_executor = process.execute_chan_sender.clone();

    let process_task = tokio::spawn(async move {
                process.run().await;
            });

    let execute_task = tokio::spawn(async move {
        if config.id == 1 {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                let op = Operation::new(0, ());
                process_executor.send(op).await.unwrap();
            }
        }
        if config.id == 2 {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                let op = Operation::new(0, ());
                process_executor.send(op).await.unwrap();
            }
        }
        
    });

    // TODO: here execute ops
    tokio::join!(network_task, process_task, execute_task);
}