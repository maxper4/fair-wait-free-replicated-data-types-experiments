use crate::dag::Dag;

#[derive(Clone)]
pub struct Operation {
    pub id: usize,
    //TODO: add arguments
}

impl Operation {
    pub fn new(id: usize) -> Operation {
        Operation { 
            id: id,
        }
    }
}

#[derive(Clone)]
pub struct VertexLabel {
    pub op: Operation,
    pub process_id: u32
}

impl VertexLabel {
    pub fn new(op_id: usize, process: u32) -> VertexLabel {
        VertexLabel {
            op: Operation::new(op_id),
            process_id: process
        }
    }

    pub fn new_from_op(op: Operation, process: u32) -> VertexLabel {
        VertexLabel {
            op: op,
            process_id: process
        }
    }
}

#[derive(Clone)]
pub struct CRDT<S: Clone, I: Iterator<Item = VertexLabel>> {
    operations: Vec<fn(S) -> S>,
    reconciliation: fn(&Dag<VertexLabel>) -> I,
    pub dag: Dag<VertexLabel>,
    initial_state: S,
}

impl <'a, S: Clone, I: Iterator<Item = VertexLabel>> CRDT<S, I> {
    pub fn new(init: S, ops: Vec<fn(S) -> S>, rec: fn(&Dag<VertexLabel>) -> I) -> CRDT<S, I> {
        CRDT { 
            operations: ops, 
            dag: Dag::new(VertexLabel::new(0, 0)),    // No process should have id 0
            reconciliation: rec,
            initial_state: init,
        }
    }

    pub fn apply(&mut self, vertex: VertexLabel) {
        let heads = self.dag.get_heads();
        self.dag.add_vertex(heads, vertex);
    }

    pub fn read(&self) -> S {
        let mut state = self.initial_state.clone();
        let mut seq = (self.reconciliation)(&self.dag);
        seq.next(); // skip the root
        for v in seq {     
            state = (self.operations[v.op.id])(state);
        }

        state
    }
}