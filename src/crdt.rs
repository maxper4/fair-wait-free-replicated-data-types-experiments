use crate::dag::Dag;

#[derive(Clone)]
pub struct CRDT<S: Clone, I: Iterator<Item = usize>> {
    operations: Vec<fn(S) -> S>,
    reconciliation: fn(&Dag<usize>) -> I,
    pub dag: Dag<usize>,
    initial_state: S,
}

impl <'a, S: Clone, I: Iterator<Item = usize>> CRDT<S, I> {
    pub fn new(init: S, ops: Vec<fn(S) -> S>, rec: fn(&Dag<usize>) -> I) -> CRDT<S, I> {
        CRDT { 
            operations: ops, 
            dag: Dag::new(0),
            reconciliation: rec,
            initial_state: init,
        }
    }

    pub fn apply(&mut self, op: usize) {
        let heads = self.dag.get_heads();
        self.dag.add_vertex(heads, op); // TODO: apply arguments
    }

    pub fn read(&self) -> S {
        let mut state = self.initial_state.clone();
        let mut seq = (self.reconciliation)(&self.dag);
        seq.next(); // skip the root
        for op in seq {     
            state = (self.operations[op])(state);
        }

        state
    }
}