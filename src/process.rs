use std::fmt::Debug;
use std::hash::{DefaultHasher, Hash, Hasher};

use crate::{OperationParameterWithRandomElement, RemoveWinsParameter};
use crate::{OperationParameterWithInitialContext, crdt::{CRDT, Operation, OperationParameter, VertexLabel}, dag::{Vertex, VertexId}, timestamp};
use rand::{Rng, SeedableRng, rngs::StdRng};
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

pub struct Process<S: Clone+Debug+'static, P> where P: OperationParameter {
    pub id: u32,
    pub crdt: CRDT<S, P>,
    in_chan: Receiver<CRDTOperationMessage<P>>,
    execute_chan_receiver: Receiver<Operation<P>>,
}

impl <'a, S: Clone+Debug+Send+'static, P> Process<S, P> where P: OperationParameter {
    pub fn new(id: u32, crdt: &CRDT<S, P>, in_chan: Receiver<CRDTOperationMessage<P>>) -> (Process<S, P>, Sender<Operation<P>>) {
        let (execute_chan_sender, execute_chan_receiver) = tokio::sync::mpsc::channel(100);

        (Process { 
            id: id, 
            crdt: crdt.clone(),
            in_chan: in_chan,
            execute_chan_receiver: execute_chan_receiver,
        }, execute_chan_sender)
    }

    pub async fn run(&mut self, out_chan: Sender<CRDTOperationMessage<P>>, metrics_out_chan: Sender<(S,u128,u128)>) {
        let mut pending = vec![];

        loop {
            select! {
                Some(m) = self.in_chan.recv() => {
                    if !self.on_receive_external_message(m.clone(), &metrics_out_chan).await {
                        // println!("Process {} cannot append {}, storing it in pending", self.id, m.vertex.label.op.id);
                        pending.push(m);
                    } else {
                        // println!("Process {} appended pending {}", self.id, m.vertex.label.op.id);
                        let mut added = true;
                        while added {
                            added = false;
                            let mut i = 0;
                            while i < pending.len() {
                                if self.on_receive_external_message(pending[i].clone(), &metrics_out_chan).await {
                                    // println!("Process {} appended pending {}", self.id, pending[i].vertex.label.op.id);
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
                    self.issue_operation(op, &out_chan, &metrics_out_chan).await;
                }
            }


            let mut added = true;
            while added {
                added = false;
                let mut i = 0;
                while i < pending.len() {
                    if self.on_receive_external_message(pending[i].clone(), &metrics_out_chan).await {
                        // println!("Process {} appended pending {}", self.id, pending[i].vertex.label.op.id);
                        pending.swap_remove(i);
                        added = true;
                    } else {
                        i += 1;
                    }
                }
            }
            // println!("Process {} exiting with {} pending messages", self.id, pending.len());

            // crate::rendering::print_graph(&self.crdt.dag, format!("process_{}.png", self.id));
            // println!("Process {} is in state {:?}", self.id, self.crdt.read());
        }
    }

    async fn on_receive_external_message(&mut self, m: CRDTOperationMessage<P>, metrics_out_chan: &Sender<(S,u128,u128)>) -> bool {
        let v = m.vertex;
        // println!("Process {} received {} from {}", self.id, v.label.op.id, v.id.process_id);
        let success = self.crdt.append_with_causal_context(v, m.causal_context); 
        
        if success {
            let now = timestamp();
            let s = self.crdt.read().clone();
            let duration = timestamp() - now;
            metrics_out_chan.send((s, now, duration)).await.unwrap();
        }

        success
    }

    async fn issue_operation(&mut self, op: Operation<P>, out_chan: &Sender<CRDTOperationMessage<P>>, metrics_out_chan: &Sender<(S,u128,u128)>) {
        match self.crdt.append(op.clone(), self.id) {
            Ok(mut causal_context) => {
                let local_id = causal_context.pop().unwrap();
                let v = self.crdt.dag.get_vertex(&local_id).unwrap().clone();

                // println!("Process {} applied {} in vertex {}", self.id, op.id, v.id);
                
                out_chan.send(CRDTOperationMessage::new(v, causal_context)).await.unwrap();

                let now = timestamp();
                let s = self.crdt.read();
                let duration = timestamp() - now;

               // tokio::spawn(async move {
                metrics_out_chan.send((s, now, duration)).await.unwrap();
               // });
            }
            Err(e) => {
                println!("Process {} cannot apply {}: {}", self.id, op.id, e);
            }
        }
    }

}

impl <'a, S: Clone+Debug+Send+'static+Hash, P> Process<S, P> where P: OperationParameterWithInitialContext {

    pub async fn run_with_initial_context(&mut self, out_chan: Sender<CRDTOperationMessage<P>>, metrics_out_chan: Sender<(S,u128,u128)>) {
        let mut pending = vec![];

        loop {
            select! {
                i = self.in_chan.recv() => {
                    match i {
                        Some(m) => {
                            println!("Process {} received {} from {}", self.id, m.vertex.label.op.id, m.vertex.id.process_id);
                            if !self.on_receive_external_message(m.clone(), &metrics_out_chan).await{
                                // println!("Process {} cannot append {}, storing it in pending", self.id, m.vertex.label.op.id);
                                pending.push(m);
                            } else {
                                // println!("Process {} appended pending {}", self.id, m.vertex.label.op.id);
                                let mut added = true;
                                while added {
                                    added = false;
                                    let mut i = 0;
                                    while i < pending.len() {
                                        if self.on_receive_external_message(pending[i].clone(), &metrics_out_chan).await {
                                            // println!("Process {} appended pending {}", self.id, pending[i].vertex.label.op.id);
                                            pending.swap_remove(i);
                                            added = true;
                                        } else {
                                            i += 1;
                                        }
                                    }
                                }
                            }
                        } None => {
                            println!("In channel closed");
                            //break;
                        }
                    }
                },
                e = self.execute_chan_receiver.recv() => {
                    match e {
                        Some(mut op) => {
                            let now = timestamp();
                            let s = self.crdt.read();
                            // println!("Issuing {} at {} with context {:?}", op.id, now, s);

                            let mut hasher = DefaultHasher::new();
                            s.hash(&mut hasher);
                            op.params.set_initial_context(now, hasher.finish(), self.id);

                            self.issue_operation(op, &out_chan, &metrics_out_chan).await;
                            println!("Process {} issued", self.id);
                        }                        
                        None => {
                            println!("Execute channel closed");
                            break;
                        }
                    }
                }
                else => {
                    println!("Panick avoided");
                    break;
                }
            }

            //crate::rendering::print_graph(&self.crdt.dag, format!("process_{}.png", self.id));
            //println!("Process {} is in state {:?}", self.id, self.crdt.read());
            if self.execute_chan_receiver.is_closed() { // no more operations to issue
                break;
            }

            //crate::rendering::print_graph(&self.crdt.dag, format!("process_{}.png", self.id));
            //println!("Process {} is in state {:?}", self.id, self.crdt.read());
        }

        drop(out_chan); // will only receive external messages now, allows to terminate nicely

        while let Some(m) = self.in_chan.recv().await {
            if !self.on_receive_external_message(m.clone(), &metrics_out_chan).await{
                // println!("Process {} cannot append {}, storing it in pending", self.id, m.vertex.label.op.id);
                pending.push(m);
            } else {
                // println!("Process {} appended pending {}", self.id, m.vertex.label.op.id);
                let mut added = true;
                while added {
                    added = false;
                    let mut i = 0;
                    while i < pending.len() {
                        if self.on_receive_external_message(pending[i].clone(), &metrics_out_chan).await {
                            // println!("Process {} appended pending {}", self.id, pending[i].vertex.label.op.id);
                            pending.swap_remove(i);
                            added = true;
                        } else {
                            i += 1;
                        }
                    }
                }
            }

            //crate::rendering::print_graph(&self.crdt.dag, format!("process_{}.png", self.id));
            //println!("Process {} is in state {:?}", self.id, self.crdt.read());                
        }

        println!("Exiting with {} pending messages and state: {:?}", pending.len(), self.crdt.read());
    }
}

impl <'a, S: Clone+Debug+Send+'static+Hash+IntoIterator<Item = (Operation<RemoveWinsParameter>, bool)>, P> Process<S, P> 
where 
    P: OperationParameterWithRandomElement,
{
    pub async fn run_with_random_element(&mut self, out_chan: Sender<CRDTOperationMessage<P>>, metrics_out_chan: Sender<(S,u128,u128)>) {
        let mut pending = vec![];

        loop {
            select! {
                i = self.in_chan.recv() => {
                    match i {
                        Some(m) => {
                            println!("Process {} received {} from {}", self.id, m.vertex.label.op.id, m.vertex.id.process_id);
                            if !self.on_receive_external_message(m.clone(), &metrics_out_chan).await{
                                // println!("Process {} cannot append {}, storing it in pending", self.id, m.vertex.label.op.id);
                                pending.push(m);
                            } else {
                                // println!("Process {} appended pending {}", self.id, m.vertex.label.op.id);
                                let mut added = true;
                                while added {
                                    added = false;
                                    let mut i = 0;
                                    while i < pending.len() {
                                        if self.on_receive_external_message(pending[i].clone(), &metrics_out_chan).await {
                                            // println!("Process {} appended pending {}", self.id, pending[i].vertex.label.op.id);
                                            pending.swap_remove(i);
                                            added = true;
                                        } else {
                                            i += 1;
                                        }
                                    }
                                }
                            }
                        } None => {
                            println!("In channel closed");
                            //break;
                        }
                    }
                },
                e = self.execute_chan_receiver.recv() => {
                    match e {
                        Some(mut op) => {
                            let now = timestamp();
                            let mut s = self.crdt.read();

                            // println!("Issuing {} at {} with context {:?}", op.id, now, s);

                            let mut rng = {
                                let rng = rand::thread_rng();
                                StdRng::from_rng(rng).unwrap()
                            };

                            let mut e = rng.gen_range(1..10);
                            let mut contained = s.into_iter().map(|(op2, applied)| 
                            if !applied || op2.params.element != e {
                                0
                                } else if op2.id == 1 {
                                    1
                                } else {
                                    -1
                            }).sum::<i32>() == 1;

                            let mut counter = 0;
                            
                            while ((contained && op.id == 1) || (!contained && op.id == 2)) && counter < 100 {
                                counter += 1;

                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                e = rng.gen_range(1..10);
                                s = self.crdt.read();

                                contained = s.into_iter().map(|(op2, applied)| 
                                if !applied || op2.params.element != e {
                                    0
                                    } else if op2.id == 1 {
                                        1
                                    } else {
                                        -1
                                }).sum::<i32>() == 1;
                            }

                            if counter >= 100 {
                                println!("Cannot find suitable random element and giving up.");
                                continue;
                            }

                            op.params.set_data(e, now, self.id);

                            self.issue_operation(op, &out_chan, &metrics_out_chan).await;
                            println!("Process {} issued", self.id);
                        }                        
                        None => {
                            println!("Execute channel closed");
                            break;
                        }
                    }
                }
                else => {
                    println!("Panick avoided");
                    break;
                }
            }

            //crate::rendering::print_graph(&self.crdt.dag, format!("process_{}.png", self.id));
            //println!("Process {} is in state {:?}", self.id, self.crdt.read());
            if self.execute_chan_receiver.is_closed() { // no more operations to issue
                break;
            }

            //crate::rendering::print_graph(&self.crdt.dag, format!("process_{}.png", self.id));
            //println!("Process {} is in state {:?}", self.id, self.crdt.read());
        }

        drop(out_chan); // will only receive external messages now, allows to terminate nicely

        while let Some(m) = self.in_chan.recv().await {
            if !self.on_receive_external_message(m.clone(), &metrics_out_chan).await{
                // println!("Process {} cannot append {}, storing it in pending", self.id, m.vertex.label.op.id);
                pending.push(m);
            } else {
                // println!("Process {} appended pending {}", self.id, m.vertex.label.op.id);
                let mut added = true;
                while added {
                    added = false;
                    let mut i = 0;
                    while i < pending.len() {
                        if self.on_receive_external_message(pending[i].clone(), &metrics_out_chan).await {
                            // println!("Process {} appended pending {}", self.id, pending[i].vertex.label.op.id);
                            pending.swap_remove(i);
                            added = true;
                        } else {
                            i += 1;
                        }
                    }
                }
            }

            //crate::rendering::print_graph(&self.crdt.dag, format!("process_{}.png", self.id));
            //println!("Process {} is in state {:?}", self.id, self.crdt.read());                
        }

        println!("Exiting with {} pending messages and state: {:?}", pending.len(), self.crdt.read());
    }
}