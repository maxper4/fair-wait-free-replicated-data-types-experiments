mod dag;
pub mod crdt;
mod process;
mod rendering;
mod config;
mod network;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::SystemTime;
use std::fmt::Debug;
use std::vec;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::hash::{DefaultHasher, Hash, Hasher};

use crdt::{Operation, OperationParameter, VertexLabel, CRDT};
use crdt::reconciliation_functions::{basic_exploration, fair_reconciliation_no_n};
use crdt::legal_functions::total;
use dag::{Dag, Vertex};

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
    fn get_initial_context(&self) -> (u128, u64, u32);
    fn set_initial_context(&mut self, time: u128, context_hash: u64, process_id: u32);
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct CommandsParameter {
    time: u128,
    initial_context_hash: u64,
    process_id: u32,
}

impl Default for CommandsParameter {
    fn default() -> Self {
        CommandsParameter {
            time: timestamp(),
            initial_context_hash: 0,
            process_id: 0,
        }
    }
}

impl OperationParameter for CommandsParameter {}

impl OperationParameterWithInitialContext for CommandsParameter {

    fn get_initial_context(&self) -> (u128, u64, u32) {
        (self.time, self.initial_context_hash, self.process_id)
    }

    fn set_initial_context(&mut self, time: u128, context: u64, process_id: u32) {
        self.time = time;
        self.initial_context_hash = context;
        self.process_id = process_id;
    }
}

pub trait OperationParameterWithRandomElement: OperationParameter {
    fn get_data(&self) -> (u32, u128, u32);
    fn set_data(&mut self, e: u32, t: u128, id: u32);
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RemoveWinsParameter {
    element: u32,
    time: u128,
    process_id: u32,
}

impl Default for RemoveWinsParameter {
    fn default() -> Self {
        RemoveWinsParameter {
            element: 0,
            time: timestamp(),
            process_id: 0,
        }
    }
}

impl OperationParameter for RemoveWinsParameter {}

impl OperationParameterWithRandomElement for RemoveWinsParameter {

    fn get_data(&self) -> (u32, u128, u32) {
        (self.element, self.time, self.process_id)
    }

    fn set_data(&mut self, e: u32, t: u128, id: u32) {
        self.element = e;
        self.time = t;
        self.process_id = id;
    }
}

pub async fn run() {
    let config = Config::get("config.toml");
    println!("Process {} launched at {} with function {} during {}s.", config.id, date(), config.reconciliation_function, config.duration);

    match config.data_type {
        1 => run_experiment_commands(config).await,
        2 => run_experiment_removewins(config).await,
        _ => run_experiment_commands(config).await,
    }
}

async fn run_experiment_commands(config: Config) {
    let (to_network_chan, from_network_chan, network_task) = network::run(&config).await;
    let (to_metrics_chan, mut from_metrics_chan) : (Sender<(Vec<Operation<CommandsParameter>>, u128, u128)>, tokio::sync::mpsc::Receiver<(Vec<Operation<CommandsParameter>>, u128, u128)>) = tokio::sync::mpsc::channel(100);

    fn mutate_commands(state: &Vec<Operation<CommandsParameter>>, op: &Operation<CommandsParameter>) -> Vec<Operation<CommandsParameter>> {
        let mut state = state.clone();
        state.push(op.clone());
        state
    }

    fn process_order(v1: &Vertex<VertexLabel<CommandsParameter>>, v2: &Vertex<VertexLabel<CommandsParameter>>) -> Ordering {
        v1.id.process_id.cmp(&v2.id.process_id)
    }

    fn commands_order(v1: &Vertex<VertexLabel<CommandsParameter>>, v2: &Vertex<VertexLabel<CommandsParameter>>) -> Ordering {
        match v1.label.op.id.cmp(&v2.label.op.id) {
            Ordering::Equal => v1.id.process_id.cmp(&v2.id.process_id),
            ord => return ord,
        }
    }

    let reconciliation = match config.reconciliation_function {
        1 => { 
            stable_reconciliation!(Vec<Operation<CommandsParameter>>, CommandsParameter, process_order, stable_commands_reconciliation);
            stable_commands_reconciliation 
        },
        2 => fair_reconciliation_no_n,
        3 => {
            crdt_reconciliation!(Vec<Operation<CommandsParameter>>, CommandsParameter, commands_order, crdt_commands_reconciliation);
            crdt_commands_reconciliation
        },
        _ => fair_reconciliation_no_n,
    };
    
    let rdt = CRDT::new(vec![], mutate_commands, reconciliation, total);
    
    let (mut process, process_executor) = Process::new(config.id, &rdt, from_network_chan);

    let process_task = tokio::spawn(async move {
            process.run_with_initial_context(to_network_chan, to_metrics_chan).await;
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

    let (to_control_metrics_chan, mut control_metrics) : (Sender<u8>, tokio::sync::mpsc::Receiver<u8>) = tokio::sync::mpsc::channel(1);

    let compute_metrics_task = tokio::spawn(async move {
        let mut metrics: HashMap<Operation<CommandsParameter>, (u128, u32, Vec<Operation<CommandsParameter>>)> = HashMap::new(); // last moved time, reordering count, previous context
        let mut computation_times: HashMap<usize, u128> = HashMap::new();

        loop {
            select! {
                Some((state, now, duration)) = from_metrics_chan.recv() => {
                    computation_times.insert(state.len(), duration);

                    for i in 0..state.len() {
                        let actual_context = state[0..i].to_vec();

                        if metrics.get(&state[i]).is_none() {
                            metrics.insert(state[i].clone(), (state[i].params.time, 0, actual_context));
                        } 
                        else if metrics.get(&state[i]).unwrap().2 != actual_context {
                            metrics.entry(state[i].clone()).and_modify(|e| {
                                e.0 = now;
                                e.1 += 1;
                                e.2 = actual_context;
                            });
                        }
                    }
                },
                Some(_) = control_metrics.recv() => {
                    break;
                }
            }
        }

        while let Some((state, now, duration)) = from_metrics_chan.recv().await {
            computation_times.insert(state.len(), duration);

            for i in 0..state.len() {
                let actual_context = state[0..i].to_vec();

                if metrics.get(&state[i]).is_none() {
                    metrics.insert(state[i].clone(), (state[i].params.time, 0, actual_context));
                } 
                else if metrics.get(&state[i]).unwrap().2 != actual_context {
                    metrics.entry(state[i].clone()).and_modify(|e| {
                        e.0 = now;
                        e.1 += 1;
                        e.2 = actual_context;
                    });
                }
            }
        }

        let sum : u128 = metrics.iter().map(|(op, (time,_,_))| time - op.params.time).sum();
        let avg = sum as f64 / metrics.len() as f64;

        println!("Average stabilization delay: {:.3} seconds over {} operations.", avg / 1000000 as f64, metrics.len());

        let total_reorderings : u32 = metrics.iter().map(|(_, (_, count, _))| *count).sum();
        let avg_reorderings = total_reorderings as f64 / metrics.len() as f64;
        println!("Average reorderings by operation: {:.3}.", avg_reorderings);

        let fairly_stabilized = metrics.iter().filter(|(op, (_, _, final_context))| { 
            let mut hasher = DefaultHasher::new();
            final_context.hash(&mut hasher);
            let final_hash = hasher.finish();
            return final_hash == op.params.initial_context_hash } );
        println!("Number of fairly stabilized operations: {} out of {}.", fairly_stabilized.clone().count() as u32, metrics.len());
        let each_fair = (1..(config.peers.len()+1)).map(|p| fairly_stabilized.clone().filter(|(op, _)| op.params.process_id == p as u32).count() as u32);
        println!("Less fair process had: {}", each_fair.clone().min().unwrap());
        println!("Most fair process had: {}", each_fair.max().unwrap());

        let sum : f64 = computation_times.iter().map(|(size, time)| *time as f64 / *size as f64).sum();
        let avg = sum as f64 / computation_times.len() as f64;
        println!("Average computation time per operation: {:.3} microseconds.", avg);

        let interests_len = vec![10, 100, 500, 1000];
        for l in interests_len {
            let t = computation_times.get(&l);
            if t.is_some() {
                println!("Computation time for state of length {}: {} microseconds.", l, t.unwrap());
            }
        }
    });

    tokio::time::timeout(tokio::time::Duration::from_secs(config.duration), execute_task).await.unwrap_err(); // experiment duration, after here the "talk" task is terminated

    println!("Process {} finished experiment at {}.", config.id, date());

    network_task.await;     // wait for all peers to finish their talking tasks, which terminates the listening tasks here
    println!("Process {} network tasks stopped.", config.id);
    process_task.await.unwrap();    // wait to finish processing all messages received (pending in the asynchronous channel from network to process)

    to_control_metrics_chan.send(0).await.unwrap();
    compute_metrics_task.await.unwrap();

    println!("Process {} stopped at {}.", config.id, date());
}

async fn run_experiment_removewins(config: Config) {
    let (to_network_chan, from_network_chan, network_task) = network::run(&config).await;
    let (to_metrics_chan, mut from_metrics_chan) : (Sender<(Vec<(Operation<RemoveWinsParameter>, bool)>, u128, u128)>, tokio::sync::mpsc::Receiver<(Vec<(Operation<RemoveWinsParameter>, bool)>, u128, u128)>) = tokio::sync::mpsc::channel(100);

    fn mutate_remove_wins_set(state: &Vec<(Operation<RemoveWinsParameter>, bool)>, op: &Operation<RemoveWinsParameter>) -> Vec<(Operation<RemoveWinsParameter>, bool)> {
        let mut state = state.clone();
        let contained = state.iter().map(|(op2, applied)| 
            if !*applied || op2.params.element != op.params.element {
                0
            } else if op2.id == 1 {
                1
            } else {
                -1
        }).sum::<i32>() == 1;

        state.push((op.clone(), (op.id == 1 && !contained) || (op.id == 2 && contained)));
        state
    }

    fn process_order(v1: &Vertex<VertexLabel<RemoveWinsParameter>>, v2: &Vertex<VertexLabel<RemoveWinsParameter>>) -> Ordering {
        v1.id.process_id.cmp(&v2.id.process_id)
    }

    fn remove_wins(v1: &Vertex<VertexLabel<RemoveWinsParameter>>, v2: &Vertex<VertexLabel<RemoveWinsParameter>>) -> Ordering {
        match v1.label.op.id.cmp(&v2.label.op.id) {     // 1 = add, 2 = remove
            Ordering::Equal => v1.id.process_id.cmp(&v2.id.process_id),
            ord => return ord,
        }
    }

    let reconciliation = match config.reconciliation_function {
        1 => {
            stable_reconciliation!(Vec<(Operation<RemoveWinsParameter>, bool)>, RemoveWinsParameter, process_order, stable_commands_reconciliation);
            stable_commands_reconciliation 
        },
        2 => fair_reconciliation_no_n,
        3 => {
            crdt_reconciliation!(Vec<(Operation<RemoveWinsParameter>, bool)>, RemoveWinsParameter, remove_wins, crdt_commands_reconciliation);
            crdt_commands_reconciliation
        },
        _ => fair_reconciliation_no_n,
    };

    let rdt = CRDT::new(vec![   // initial state is [0, 2, 4, 6, 8]
        (Operation::new(1, RemoveWinsParameter { element: 0, time: 0, process_id: 0 }), true),
        (Operation::new(1, RemoveWinsParameter { element: 2, time: 0, process_id: 0 }), true),
        (Operation::new(1, RemoveWinsParameter { element: 4, time: 0, process_id: 0 }), true),
        (Operation::new(1, RemoveWinsParameter { element: 6, time: 0, process_id: 0 }), true),
        (Operation::new(1, RemoveWinsParameter { element: 8, time: 0, process_id: 0 }), true)

    ], mutate_remove_wins_set, reconciliation, total);

    let (mut process, process_executor) = Process::new(config.id, &rdt, from_network_chan);

    let process_task = tokio::spawn(async move {
            process.run_with_random_element(to_network_chan, to_metrics_chan).await;
        });

    let execute_task = async move {
        let mut rng = {
            let rng = rand::thread_rng();
            StdRng::from_rng(rng).unwrap()
        };

        if config.id != 1 {
            loop {
                let delay = rng.gen_range(1..5);
                tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;

                let addremove = rng.gen_range(1..3); // 1 = add, 2 = remove
                let op = Operation::new(addremove, RemoveWinsParameter::default());
                process_executor.send(op).await.unwrap();
            }
        }
        if config.id == 1 {
            loop {
                let delay = rng.gen_range(5..10);
                tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;

                let addremove = rng.gen_range(1..3); // 1 = add, 2 = remove
                let op = Operation::new(addremove, RemoveWinsParameter::default());
                process_executor.send(op).await.unwrap();
            }
        }
    };

    let (to_control_metrics_chan, mut control_metrics) : (Sender<u8>, tokio::sync::mpsc::Receiver<u8>) = tokio::sync::mpsc::channel(1);

    let compute_metrics_task = tokio::spawn(async move {
        let mut metrics: HashMap<Operation<RemoveWinsParameter>, (u128, u32, bool)> = HashMap::new(); // last changed time, result change count, previous result
        let mut computation_times: HashMap<usize, u128> = HashMap::new();

        loop {
            select! {
                Some((state, now, duration)) = from_metrics_chan.recv() => {
                    computation_times.insert(state.len(), duration);

                    for i in 5..state.len() {
                        let (op, result) = state[i].clone();
                        if metrics.get(&op).is_none() {
                            metrics.insert(op.clone(), (op.params.time, 0, result));
                        } 
                        else if metrics.get(&op).unwrap().2 != result {
                            metrics.entry(op.clone()).and_modify(|e| {
                                e.0 = now;
                                e.1 += 1;
                                e.2 = result;
                            });
                        }
                    }
                },
                Some(_) = control_metrics.recv() => {
                    break;
                }
            }
        }

        while let Some((state, now, duration)) = from_metrics_chan.recv().await {
            computation_times.insert(state.len(), duration);

            for i in 5..state.len() {
                let (op, result) = state[i].clone();
                if metrics.get(&op).is_none() {
                    metrics.insert(op.clone(), (op.params.time, 0, result));
                } 
                else if metrics.get(&op).unwrap().2 != result {
                    metrics.entry(op.clone()).and_modify(|e| {
                        e.0 = now;
                        e.1 += 1;
                        e.2 = result;
                    });
                }
            }
        }

        let sum : u128 = metrics.iter().map(|(op, (time,_,_))| time - op.params.time).sum();
        let avg = sum as f64 / metrics.len() as f64;

        println!("Average stabilization delay: {:.3} seconds over {} operations.", avg / 1000000 as f64, metrics.len());

        let total_reorderings : u32 = metrics.iter().map(|(_, (_, count, _))| *count).sum();
        let avg_reorderings = total_reorderings as f64 / metrics.len() as f64;
        println!("Average reorderings by operation: {:.3}.", avg_reorderings);

        let fairly_stabilized = metrics.iter().filter(|(_, (_, _, last_result))| { 
            return *last_result } );
        println!("Number of fairly stabilized operations: {} out of {}.", fairly_stabilized.clone().count() as u32, metrics.len());
        let each_fair = (1..(config.peers.len()+1)).map(|p| fairly_stabilized.clone().filter(|(op, _)| op.params.process_id == p as u32).count() as u32);
        println!("Less fair process had: {}", each_fair.clone().min().unwrap());
        println!("Most fair process had: {}", each_fair.max().unwrap());

        let sum : f64 = computation_times.iter().map(|(size, time)| *time as f64 / *size as f64).sum();
        let avg = sum as f64 / computation_times.len() as f64;
        println!("Average computation time per operation: {:.3} microseconds.", avg);

        let interests_len = vec![10, 100, 500, 1000];
        for l in interests_len {
            let t = computation_times.get(&l);
            if t.is_some() {
                println!("Computation time for state of length {}: {} microseconds.", l, t.unwrap());
            }
        }
    });

    tokio::time::timeout(tokio::time::Duration::from_secs(config.duration), execute_task).await.unwrap_err(); // experiment duration, after here the "talk" task is terminated

    println!("Process {} finished experiment at {}.", config.id, date());

    network_task.await;     // wait for all peers to finish their talking tasks, which terminates the listening tasks here
    println!("Process {} network tasks stopped.", config.id);
    process_task.await.unwrap();    // wait to finish processing all messages received (pending in the asynchronous channel from network to process)

    to_control_metrics_chan.send(0).await.unwrap();
    compute_metrics_task.await.unwrap();

    println!("Process {} stopped at {}.", config.id, date());
}