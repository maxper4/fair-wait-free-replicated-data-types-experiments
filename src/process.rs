use core::fmt;
use std::fmt::{Debug, Display};

use crate::{crdt::{Operation, OperationParameter, VertexLabel, CRDT}, dag::{Vertex, VertexId}};
use tokio::{select, sync::mpsc::{Receiver, Sender}};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CRDTOperationMessage<P> where P: OperationParameter {
    pub vertex: Vertex<VertexLabel<P>>,
    pub causal_context: Vec<VertexId>,
}

impl <P>CRDTOperationMessage<P> where P: OperationParameter {
    pub fn new(vertex: Vertex<VertexLabel<P>>, causal_context: Vec<VertexId>) -> CRDTOperationMessage<P> {
        CRDTOperationMessage {
            vertex: vertex,
            causal_context: causal_context
        }
    }

    pub fn to_string(&self) -> String {
        format!(
            "CRDTOperationMessage: (vertex: {}, causal_context: {})",
            self.vertex.to_string(),
            self.causal_context.iter()
                .map(|id| id.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

pub struct Process<S: Clone+Debug, P> where P: OperationParameter {
    pub id: u32,
    pub crdt: CRDT<S, P>,
    in_chan: Receiver<CRDTOperationMessage<P>>,
    pub execute_chan_sender: Sender<Operation<P>>,
    execute_chan_receiver: Receiver<Operation<P>>,
    pub control_chan_sender: Sender<u8>,
    control_chan_receiver: Receiver<u8>,
}

impl <'a, S: Clone, P> Process<S, P> where S: Debug, P: OperationParameter {
    pub fn new(id: u32, crdt: &CRDT<S, P>, in_chan: Receiver<CRDTOperationMessage<P>>) -> Process<S, P> {
        let (execute_chan_sender, execute_chan_receiver) = tokio::sync::mpsc::channel(100);
        let (control_chan_sender, control_chan_receiver) = tokio::sync::mpsc::channel(5);

        Process { 
            id: id, 
            crdt: crdt.clone(),
            in_chan: in_chan,
            execute_chan_sender: execute_chan_sender,
            execute_chan_receiver: execute_chan_receiver,
            control_chan_sender: control_chan_sender,
            control_chan_receiver: control_chan_receiver,
        }
    }

    pub async fn run(&mut self, out_chan: Sender<CRDTOperationMessage<P>>) {
        let mut pending = vec![];

        loop {
            select! {
                Some(m) = self.in_chan.recv() => {
                    if !self.on_receive_external_message(m.clone()) {
                        println!("Process {} cannot append {}, storing it in pending", self.id, m.vertex.label.op.id);
                        pending.push(m);
                    } else {
                        println!("Process {} appended pending {}", self.id, m.vertex.label.op.id);
                        let mut added = true;
                        while added {
                            added = false;
                            let mut i = 0;
                            while i < pending.len() {
                                if self.on_receive_external_message(pending[i].clone()) {
                                    println!("Process {} appended pending {}", self.id, pending[i].vertex.label.op.id);
                                    pending.swap_remove(i);
                                    added = true;
                                } else {
                                    i += 1;
                                }
                            }
                        }
                    }
                },
                Some(op) = self.execute_chan_receiver.recv() => {
                    self.issue_operation(op, &out_chan).await;
                }
            }

            let mut added = true;
            while added {
                added = false;
                let mut i = 0;
                while i < pending.len() {
                    if self.on_receive_external_message(pending[i].clone()) {
                        println!("Process {} appended pending {}", self.id, pending[i].vertex.label.op.id);
                        pending.swap_remove(i);
                        added = true;
                    } else {
                        i += 1;
                    }
                }
            }
            println!("Process {} exiting with {} pending messages", self.id, pending.len());

            crate::rendering::print_graph(&self.crdt.dag, format!("process_{}.png", self.id));
            println!("Process {} is in state {:?}", self.id, self.crdt.read());
        }
    }

    fn on_receive_external_message(&mut self, m: CRDTOperationMessage<P>) -> bool {   // TODO this should run in the background to allow appending the causal past received later
        let v = m.vertex;
        println!("Process {} received {} from {}", self.id, v.label.op.id, v.id.process_id);
        self.crdt.append_with_causal_context(v.clone(), m.causal_context.clone())
    }

    async fn issue_operation(&mut self, op: Operation<P>, out_chan: &Sender<CRDTOperationMessage<P>>) {
        match self.crdt.append(op.clone(), self.id) {
            Ok(mut causal_context) => {
                let local_id = causal_context.pop().unwrap();
                let v = self.crdt.dag.get_vertex(&local_id).unwrap().clone();

                println!("Process {} applied {}", self.id, op.id);
                
                match out_chan.send(CRDTOperationMessage::new(v, causal_context)).await {
                    Ok(_) => (),
                    Err(e) => println!("Process {} cannot send {} to network: {}", self.id, op.id, e),
                }
            }
            Err(e) => {
                println!("Process {} cannot apply {}: {}", self.id, op.id, e);
            }
        }
    }

    pub fn metrics(&self) {
        println!("Process {} metrics: DAG length: {}", self.id, self.crdt.dag.length);
    }
}