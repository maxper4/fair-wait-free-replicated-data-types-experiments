use crate::crdt::{VertexLabel, Operation, CRDT};
use tokio::{select, sync::mpsc::{Receiver, Sender}};

pub struct Process<S: Clone, I: Iterator<Item = VertexLabel>> where I:Clone {
    pub id: u32,
    crdt: CRDT<S, I>,
    in_chan: Receiver<VertexLabel>,
    out_chan: Sender<VertexLabel>,
    pub execute_chan_sender: Sender<Operation>,
    execute_chan_receiver: Receiver<Operation>,
}

impl <'a, S: Clone, I: Iterator<Item = VertexLabel>> Process<S, I> where I:Clone {
    pub fn new(id: u32, crdt: &CRDT<S, I>, in_chan: Receiver<VertexLabel>, out_chan: &Sender<VertexLabel>) -> Process<S, I> {
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
                Some(v) = self.in_chan.recv() => {
                    if v.process_id != self.id {
                        println!("Process {} received {} from {}", self.id, v.op.id, v.process_id);
                        self.crdt.apply(v);
                    }
                }
                Some(op) = self.execute_chan_receiver.recv() => {
                    let v = VertexLabel::new_from_op(op.clone(), self.id);
                    self.crdt.apply(v.clone());
                    println!("Process {} applied {}", self.id, v.op.id);

                    self.out_chan.send(v).await;
                }
            }
        }
    }
}