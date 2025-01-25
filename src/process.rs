use crate::crdt::CRDT;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;

pub struct Process<S: Clone, I: Iterator<Item = usize>> where I:Clone {
    pub id: usize,
    crdt: CRDT<S, I>,
    in_chan: Receiver<usize>,
    out_chan: Sender<usize>,
}

impl <'a, S: Clone, I: Iterator<Item = usize>> Process<S, I> where I:Clone {
    pub fn new(id: usize, crdt: &CRDT<S, I>, in_chan: Receiver<usize>, out_chan: &Sender<usize>) -> Process<S, I> {
        Process { 
            id: id, 
            crdt: crdt.clone(),
            in_chan: in_chan,
            out_chan: out_chan.clone(),
        }
    }

    pub fn run(&mut self) { // TODO: use async to read incoming op and apply some
        self.crdt.apply(0);
        self.out_chan.send(0).unwrap();
        self.crdt.apply(0);
        self.out_chan.send(0).unwrap();

        for i in self.in_chan.iter() {  // TODO: dont apply self sent operations
            println!("Process {} received {}", self.id, i);
            self.crdt.apply(i);
        }
    }
}