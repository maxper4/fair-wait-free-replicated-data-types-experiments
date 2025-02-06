use crate::crdt::{VertexLabel, CRDT};
use std::sync::mpsc::{Sender, Receiver};

pub struct Process<S: Clone, I: Iterator<Item = VertexLabel>> where I:Clone {
    pub id: u32,
    crdt: CRDT<S, I>,
    in_chan: Receiver<VertexLabel>,
    out_chan: Sender<VertexLabel>,
}

impl <'a, S: Clone, I: Iterator<Item = VertexLabel>> Process<S, I> where I:Clone {
    pub fn new(id: u32, crdt: &CRDT<S, I>, in_chan: Receiver<VertexLabel>, out_chan: &Sender<VertexLabel>) -> Process<S, I> {
        Process { 
            id: id, 
            crdt: crdt.clone(),
            in_chan: in_chan,
            out_chan: out_chan.clone(),
        }
    }

    pub fn run(&mut self) { // TODO: use async to read incoming op and apply some
        self.crdt.apply(VertexLabel::new(0, self.id));
        self.out_chan.send(VertexLabel::new(0, self.id)).unwrap();
        self.crdt.apply(VertexLabel::new(0, self.id));
        self.out_chan.send(VertexLabel::new(0, self.id)).unwrap();

        for i in self.in_chan.iter() {
            if i.process_id != self.id {
                println!("Process {} received {} from {}", self.id, i.op.id, i.process_id);
                self.crdt.apply(i);
            }
        }
    }
}