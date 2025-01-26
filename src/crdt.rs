use crate::dag::Dag;

#[derive(Clone)]
pub struct Operation {
    pub id: usize,
    //TODO: add arguments
}

impl Operation {
    pub fn new(id: usize) -> Operation {
        Operation { id }
    }
}

#[derive(Clone)]
pub struct CRDT<S: Clone, I: Iterator<Item = Operation>> {
    operations: Vec<fn(S) -> S>,
    reconciliation: fn(&Dag<Operation>) -> I,
    pub dag: Dag<Operation>,
    initial_state: S,
}

impl <'a, S: Clone, I: Iterator<Item = Operation>> CRDT<S, I> {
    pub fn new(init: S, ops: Vec<fn(S) -> S>, rec: fn(&Dag<Operation>) -> I) -> CRDT<S, I> {
        CRDT { 
            operations: ops, 
            dag: Dag::new(Operation::new(0)),
            reconciliation: rec,
            initial_state: init,
        }
    }

    pub fn apply(&mut self, op: Operation) {
        let heads = self.dag.get_heads();
        self.dag.add_vertex(heads, op);
    }

    pub fn read(&self) -> S {
        let mut state = self.initial_state.clone();
        let mut seq = (self.reconciliation)(&self.dag);
        seq.next(); // skip the root
        for op in seq {     
            state = (self.operations[op.id])(state);
        }

        state
    }
}