use crate::crdt::{Operation, CRDT};
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;

pub struct Process<S: Clone, I: Iterator<Item = Operation>> where I:Clone {
    pub id: usize,
    crdt: CRDT<S, I>,
    in_chan: Receiver<Operation>,
    out_chan: Sender<Operation>,
}

impl <'a, S: Clone, I: Iterator<Item = Operation>> Process<S, I> where I:Clone {
    pub fn new(id: usize, crdt: &CRDT<S, I>, in_chan: Receiver<Operation>, out_chan: &Sender<Operation>) -> Process<S, I> {
        Process { 
            id: id, 
            crdt: crdt.clone(),
            in_chan: in_chan,
            out_chan: out_chan.clone(),
        }
    }

    pub fn run(&mut self) { // TODO: use async to read incoming op and apply some
        self.crdt.apply(Operation::new(0));
        self.out_chan.send(Operation::new(0)).unwrap();
        self.crdt.apply(Operation::new(0));
        self.out_chan.send(Operation::new(0)).unwrap();

        for i in self.in_chan.iter() {  // TODO: dont apply self sent operations
            println!("Process {} received {}", self.id, i.id);
            self.crdt.apply(i);
        }
    }
}