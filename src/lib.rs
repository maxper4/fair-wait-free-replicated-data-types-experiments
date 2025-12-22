mod dag;
pub mod crdt;
mod process;
mod rendering;
mod config;
mod network;

use std::cmp::Ordering;
use std::time::SystemTime;
use std::vec;
use std::time::{Duration, SystemTime};

use crdt::{Operation, OperationParameter, VertexLabel, CRDT};
use crdt::reconciliation_functions::{basic_exploration, fair_reconciliation_no_n};
use crdt::legal_functions::total;
use dag::{Dag, Vertex, VertexId};

use config::Config;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

use crate::process::CRDTOperationMessage;

fn timestamp() -> u128 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros()
}

fn date() -> String {
    let now = SystemTime::now();
    let datetime: chrono::DateTime<chrono::Utc> = now.into();
    datetime.format("%H:%M:%S%.6f").to_string()
}

pub async fn run() {
    let config = Config::get("config.toml");
    let wakeup_time = timestamp();
    println!("Process {} launched at {}.", config.id, wakeup_time);

    let (to_network_chan, from_network_chan, network_task) = network::run(&config).await;

    fn mutate_counter(state: &u32, op: &Operation<CounterParameter>) -> u32 {
        *state + op.params.inc
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct CounterParameter {
        inc: u32,
        time: u128,
    }

    impl Default for CounterParameter {
        fn default() -> Self {
            CounterParameter {
                inc: 1,
                time: timestamp(),
            }
        }
    }

    impl OperationParameter for CounterParameter {}

    let counter = CRDT::new(0, mutate_counter, basic_exploration, total);
    let mut process = Process::new(config.id, &counter, from_network_chan);
    let process_executor = process.execute_chan_sender.clone();

    let process_task = tokio::spawn(async move {
                process.run(to_network_chan).await;
            });

    let execute_task = tokio::spawn(async move {
        if config.id != 1 {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                let now = timestamp();
                let op = Operation::new(0, CounterParameter { inc: 1, time: now });
                process_executor.send(op).await.unwrap();
            }
        }
        if config.id == 1 {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                let now = timestamp();
                let op = Operation::new(0, CounterParameter { inc: 10, time: now });
                process_executor.send(op).await.unwrap();
            }
        }
        
    });

    tokio::time::timeout(tokio::time::Duration::from_secs(30), async { tokio::join!(process_task, execute_task) }).await;

    network_task.await;     // wait for all peers to finish their talking tasks, which terminates the listening tasks here
    println!("Process {} network tasks stopped.", config.id);
    process_task.await.unwrap();    // wait to finish processing all messages received (pending in the asynchronous channel from network to process)
    
    let now = timestamp();
    println!("Process {} stopped at {}.", config.id, now);
}