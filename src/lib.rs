mod dag;
pub mod crdt;
mod process;
mod rendering;
mod config;
mod network;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::SystemTime;
use std::vec;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::time::{Duration, SystemTime};

use crdt::{Operation, OperationParameter, VertexLabel, CRDT};
use crdt::reconciliation_functions::{basic_exploration, fair_reconciliation_no_n};
use crdt::legal_functions::total;
use dag::{Dag, Vertex, VertexId};

use config::Config;
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::sync::mpsc::Sender;

use crate::process::{CRDTOperationMessage, Process};

fn timestamp() -> u128 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros()
}

fn date() -> String {
    let now = SystemTime::now();
    let datetime: chrono::DateTime<chrono::Utc> = now.into();
    datetime.format("%H:%M:%S%.6f").to_string()
}

pub trait OperationParameterWithInitialContext: OperationParameter {
    type S;

    fn get_initial_context(&self) -> (u128, Self::S);
    fn set_initial_context(&mut self, time: u128, context: Self::S);
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
    pub struct CommandsParameter {
        time: u128,
        initial_context: Vec<Operation<CommandsParameter>>,
    }

    impl Default for CommandsParameter {
        fn default() -> Self {
            CommandsParameter {
                time: timestamp(),
                initial_context: vec![],
            }
        }
    }

impl OperationParameter for CommandsParameter {}

impl OperationParameterWithInitialContext for CommandsParameter {
    type S = Vec<Operation<CommandsParameter>>;

    fn get_initial_context(&self) -> (u128, Vec<Operation<CommandsParameter>>) {
        (self.time, self.initial_context.clone())
    }

    fn set_initial_context(&mut self, time: u128, context: Vec<Operation<CommandsParameter>>) {
        self.initial_context = context;
        self.time = time;
    }
}

pub async fn run() {
    let config = Config::get("config.toml");
    let wakeup_time = timestamp();
    println!("Process {} launched at {} for experiment type {}.", config.id, wakeup_time, config.experiment_type);

    let (to_network_chan, from_network_chan, network_task) = network::run(&config).await;
    let (to_metrics_chan, mut from_metrics_chan) : (Sender<(Vec<Operation<CommandsParameter>>, u128)>, tokio::sync::mpsc::Receiver<(Vec<Operation<CommandsParameter>>, u128)>) = tokio::sync::mpsc::channel(100);

    fn mutate_commands(state: &Vec<Operation<CommandsParameter>>, op: &Operation<CommandsParameter>) -> Vec<Operation<CommandsParameter>> {
        let mut state = state.clone();
        state.push(op.clone());
        state
    }

    

    fn commands_order(v1: &Vertex<VertexLabel<CommandsParameter>>, v2: &Vertex<VertexLabel<CommandsParameter>>) -> Ordering {
        match v1.label.op.id.cmp(&v2.label.op.id) {
            Ordering::Equal => v1.id.process_id.cmp(&v2.id.process_id),
            ord => return ord,
        }
    }

    stable_reconciliation!(Vec<Operation<CommandsParameter>>, CommandsParameter, commands_order, stable_commands_reconciliation);

    let commands = CRDT::new(vec![], mutate_commands, stable_commands_reconciliation, total);
    let mut process = Process::new(config.id, &commands, from_network_chan, &to_metrics_chan);
    let process_executor = process.execute_chan_sender.clone();

    let process_task = tokio::spawn(async move {
                process.run_with_initial_context(&to_network_chan).await;
            });

    let execute_task = async move {
        let mut counter = 1;

        let mut rng = {
            let rng = rand::thread_rng();
            StdRng::from_rng(rng).unwrap()
        };

        if config.id != 1 {
            loop {
                let delay = rng.gen_range(1..5);
                tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;

                let op = Operation::new(counter, CommandsParameter::default());
                process_executor.send(op).await.unwrap();
                counter += 1;
            }
        }
        if config.id == 1 {
            loop {
                let delay = rng.gen_range(5..10);
                tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;

                let op = Operation::new(counter, CommandsParameter::default());
                process_executor.send(op).await.unwrap();
                counter += 1;
            }
        }
        
    };

    let (to_control_metrics_chan, mut control_metrics) : (Sender<u8>, tokio::sync::mpsc::Receiver<u8>) = tokio::sync::mpsc::channel(10);

    let compute_metrics_task = tokio::spawn(async move {
        let mut metrics: HashMap<Operation<CommandsParameter>, (u128, u32, Vec<Operation<CommandsParameter>>)> = HashMap::new(); // last moved time, reordering count, previous context

        // TODO initial context need to be flatten to just ids to avoid recursive structures: solution is to hash the context?

        loop {
            select! {
                Some((state, now)) = from_metrics_chan.recv() => {

                    for i in 0..state.len() {
                        if metrics.get(&state[i]).is_none() {
                            metrics.insert(state[i].clone(), (state[i].params.time, 0, state[0..i].to_vec()));
                        } 
                        else if metrics.get(&state[i]).unwrap().2 != state[0..i].to_vec() {
                            metrics.entry(state[i].clone()).and_modify(|e| {
                                e.0 = now;
                                e.1 += 1;
                                e.2 = state[0..i].to_vec();
                            });
                        }
                    }
                },
                Some(_) = control_metrics.recv() => {
                    break;
                }
            }
        }

        let sum : u128 = metrics.iter().map(|(op, (time,_,_))| time - op.params.time).sum();
        let avg = sum as f64 / metrics.len() as f64;

        println!("Average time for process {}: {} seconds over {} operations.", config.id, avg / 1000000 as f64, metrics.len());

        let total_reorderings : u32 = metrics.iter().map(|(_, (_, count, _))| *count).sum();
        let avg_reorderings = total_reorderings as f64 / metrics.len() as f64;
        println!("Average reorderings by operation for process {}: {}.", config.id, avg_reorderings);

        let fairly_stabilized : u32 = metrics.iter().filter(|(_, (_, count, _))| *count == 0).count() as u32;  // TODO need to compare final context with initial context stored in the operation itself
        println!("Number of fairly stabilized operations for process {}: {} out of {}.", config.id, fairly_stabilized, metrics.len());
    });

    tokio::time::timeout(tokio::time::Duration::from_secs(30), execute_task).await;

    println!("Process {} finished experiment at {}.", config.id, timestamp());

    network_task.await;     // wait for all peers to finish their talking tasks, which terminates the listening tasks here
    println!("Process {} network tasks stopped.", config.id);
    process_task.await.unwrap();    // wait to finish processing all messages received (pending in the asynchronous channel from network to process)
    
    to_control_metrics_chan.send(0).await.unwrap();
    compute_metrics_task.await.unwrap();

    println!("Process {} stopped at {}.", config.id, timestamp());
}