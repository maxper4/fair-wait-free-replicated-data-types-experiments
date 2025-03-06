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
    out_chan: Sender<CRDTOperationMessage<P>>,
    pub execute_chan_sender: Sender<Operation<P>>,
    execute_chan_receiver: Receiver<Operation<P>>,
}

impl <'a, S: Clone+Debug, P> Process<S, P> where P: OperationParameter {
    pub fn new(id: u32, crdt: &CRDT<S, P>, in_chan: Receiver<CRDTOperationMessage<P>>, out_chan: &Sender<CRDTOperationMessage<P>>) -> Process<S, P> {
        let (execute_chan_sender, execute_chan_receiver) = tokio::sync::mpsc::channel(100);
        Process { 
            id: id, 
            crdt: crdt.clone(),
            in_chan: in_chan,
            out_chan: out_chan.clone(),
            execute_chan_sender: execute_chan_sender,
            execute_chan_receiver: execute_chan_receiver,
        }
    }

    pub async fn run(&mut self) {
        loop {
            select! {
                Some(m) = self.in_chan.recv() => {
                    let v = m.vertex;
                    if v.id.process_id != self.id {
                        println!("Process {} received {} from {}", self.id, v.label.op.id, v.id.process_id);
                        self.crdt.append_with_causal_context(v, m.causal_context);
                    } else {
                        println!("Process {} received its own operation", self.id);
                    }
                }
                Some(op) = self.execute_chan_receiver.recv() => {
                    match self.crdt.append(op.clone(), self.id) {
                        Ok(mut causal_context) => {
                            let local_id = causal_context.pop().unwrap();
                            let v = self.crdt.dag.get_vertex(&local_id).unwrap().clone();

                            println!("Process {} applied {}", self.id, op.id);

                            let out_clone = self.out_chan.clone();
                            tokio::spawn(async move {
                                out_clone.send(CRDTOperationMessage::new(v, causal_context)).await.unwrap();
                            });
                        }
                        Err(e) => {
                            println!("Process {} cannot apply {}: {}", self.id, op.id, e);
                        }
                    }
                    
                }
            }

            crate::rendering::print_graph(&self.crdt.dag, format!("process_{}.png", self.id));
            println!("Process {} is in state {:?}", self.id, self.crdt.read());

        }
    }
}