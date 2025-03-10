use crate::dag::{Dag, Vertex, VertexId};

pub trait OperationParameter: Clone + Send + PartialEq + Eq + Default + 'static {}

impl OperationParameter for () {}

#[derive(Clone)]
pub struct Operation<P> where P: OperationParameter {    
    pub id: usize,
    pub params: P,
}

impl <P>Operation<P> where P: OperationParameter {
    pub fn new(id: usize, params: P) -> Operation<P>  {
        Operation { 
            id: id,
            params: params
        }
    }
}

#[derive(Clone)]
pub struct VertexLabel<P> where P: OperationParameter {
    pub op: Operation<P>,
    pub process_id: u32     // TODO: we store 2 times the process id, one in the vertex id and one here
}

impl <P>VertexLabel<P> where P: OperationParameter {
    pub fn new(op_id: usize, params: P, process: u32) -> VertexLabel<P> {
        VertexLabel {
            op: Operation::new(op_id, params),
            process_id: process
        }
    }

    pub fn new_from_op(op: Operation<P>, process: u32) -> VertexLabel<P> {
        VertexLabel {
            op: op,
            process_id: process
        }
    }
}

#[derive(Clone)]
pub struct CRDT<S: Clone, I: Iterator<Item = VertexLabel<P>> + Clone, P: OperationParameter> {
    operations: Vec<fn(S, P) -> S>,
    reconciliation: fn(&Dag<VertexLabel<P>>) -> I,
    pub dag: Dag<VertexLabel<P>>,
    initial_state: S,
    local_id: usize,
}

impl <S: Clone, I: Iterator<Item = VertexLabel<P>> + Clone, P: OperationParameter> CRDT<S, I, P> {
    pub fn new(init: S, ops: Vec<fn(S, P) -> S>, rec: fn(&Dag<VertexLabel<P>>) -> I) -> CRDT<S, I, P> {
        CRDT { 
            operations: ops, 
            dag: Dag::new(VertexLabel::new(0, P::default(), 0)),    // No process should have id 0
            reconciliation: rec,
            initial_state: init,
            local_id: 1,
        }
    }

    pub fn apply(&mut self, op: Operation<P>, from: u32) -> Vec<VertexId> {
        let mut heads = self.dag.get_heads();
        let id = VertexId::new(self.local_id, from);
        self.local_id += 1;
        let v = Vertex::new(id, VertexLabel::new_from_op(op, from));
        self.dag.add_vertex(heads.clone(), v);
        heads.push(id);
        heads
    }

    pub fn apply_with_causal_context(&mut self, vertex: Vertex<VertexLabel<P>>, causal_context: Vec<VertexId>) {
        self.dag.add_vertex(causal_context, vertex);
    }

    pub fn read(&self) -> S {
        let mut state = self.initial_state.clone();
        let mut seq = (self.reconciliation)(&self.dag);
        seq.next(); // skip the root
        for v in seq {     
            state = (self.operations[v.op.id])(state, v.op.params.clone());
        }

        state
    }
}