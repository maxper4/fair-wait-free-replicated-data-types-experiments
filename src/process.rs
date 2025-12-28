use std::fmt::Debug;

use crate::{crdt::{Operation, OperationParameter, VertexLabel, CRDT}, dag::{Vertex, VertexId}};
use tokio::{select, sync::mpsc::{Receiver, Sender}};

#[derive(Clone)]
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
}

pub struct Process<S: Clone, P> where S: Debug, P: OperationParameter {
    pub id: u32,
    pub crdt: CRDT<S, P>,
    in_chan: Receiver<CRDTOperationMessage<P>>,
    pub execute_chan_sender: Sender<Operation<P>>,
    execute_chan_receiver: Receiver<Operation<P>>,
}

impl <'a, S: Clone, P> Process<S, P> where S: Debug, P: OperationParameter {
    pub fn new(id: u32, crdt: &CRDT<S, P>, in_chan: Receiver<CRDTOperationMessage<P>>) -> Process<S, P> {
        let (execute_chan_sender, execute_chan_receiver) = tokio::sync::mpsc::channel(100);
        Process { 
            id: id, 
            crdt: crdt.clone(),
            in_chan: in_chan,
            execute_chan_sender: execute_chan_sender,
            execute_chan_receiver: execute_chan_receiver,
        }
    }

    pub async fn run(&mut self, out_chan: Sender<CRDTOperationMessage<P>>) {
        loop {
            select! {
                Some(m) = self.in_chan.recv() => {
                    self.on_receive_external_message(m).await;
                }
                Some(op) = self.execute_chan_receiver.recv() => {
                    self.issue_operation(op, &out_chan).await;
                }
            }

            crate::rendering::print_graph(&self.crdt.dag, format!("process_{}.png", self.id));
            println!("Process {} is in state {:?}", self.id, self.crdt.read());
        }
    }

    async fn on_receive_external_message(&mut self, m: CRDTOperationMessage<P>) {
        let v = m.vertex;
        println!("Process {} received {} from {}", self.id, v.label.op.id, v.id.process_id);
        self.crdt.append_with_causal_context(v, m.causal_context);
    }

    async fn issue_operation(&mut self, op: Operation<P>, out_chan: &Sender<CRDTOperationMessage<P>>) {
        match self.crdt.append(op.clone(), self.id) {
            Ok(mut causal_context) => {
                let local_id = causal_context.pop().unwrap();
                let v = self.crdt.dag.get_vertex(&local_id).unwrap().clone();

                println!("Process {} applied {}", self.id, op.id);
                
                out_chan.send(CRDTOperationMessage::new(v, causal_context)).await.unwrap();
            }
            Err(e) => {
                println!("Process {} cannot apply {}: {}", self.id, op.id, e);
            }
        }
    }
}