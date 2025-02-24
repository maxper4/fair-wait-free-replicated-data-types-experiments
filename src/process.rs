use crate::{crdt::{Operation, VertexLabel, CRDT}, dag::{Vertex, VertexId}};
use tokio::{select, sync::mpsc::{Receiver, Sender}};

#[derive(Clone)]
pub struct CRDTOperationMessage {
    pub vertex: Vertex<VertexLabel>,
    pub causal_context: Vec<VertexId>,
}

impl CRDTOperationMessage {
    pub fn new(vertex: Vertex<VertexLabel>, causal_context: Vec<VertexId>) -> CRDTOperationMessage {
        CRDTOperationMessage {
            vertex: vertex,
            causal_context: causal_context
        }
    }
}

pub struct Process<S: Clone, I: Iterator<Item = VertexLabel>> where I:Clone {
    pub id: u32,
    pub crdt: CRDT<S, I>,
    in_chan: Receiver<CRDTOperationMessage>,
    out_chan: Sender<CRDTOperationMessage>,
    pub execute_chan_sender: Sender<Operation>,
    execute_chan_receiver: Receiver<Operation>,
}

impl <'a, S: Clone, I: Iterator<Item = VertexLabel>> Process<S, I> where I:Clone {
    pub fn new(id: u32, crdt: &CRDT<S, I>, in_chan: Receiver<CRDTOperationMessage>, out_chan: &Sender<CRDTOperationMessage>) -> Process<S, I> {
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
                        self.crdt.apply_with_causal_context(v, m.causal_context);
                    }
                }
                Some(op) = self.execute_chan_receiver.recv() => {
                    let mut causal_context = self.crdt.apply(op.clone(), self.id);
                    let local_id = causal_context.pop().unwrap();
                    let v = self.crdt.dag.get_vertex(local_id).unwrap().clone();

                    println!("Process {} applied {}", self.id, op.id);

                    let out_clone = self.out_chan.clone();
                    tokio::spawn(async move {
                        out_clone.send(CRDTOperationMessage::new(v, causal_context)).await.unwrap();
                    });
                }
            }

            crate::rendering::print_graph(&self.crdt.dag, format!("process_{}.png", self.id));

        }
    }
}