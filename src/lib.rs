mod dag;
pub mod crdt;
mod process;
mod rendering;
mod config;

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
}