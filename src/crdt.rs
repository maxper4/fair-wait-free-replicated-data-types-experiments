pub mod reconciliation_functions;
pub mod legal_functions;

use crate::{crdt::legal_functions::IllegalOperationError, dag::{Dag, Vertex, VertexId}, mutate_if_legal, stable_reconciliation};

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
}

impl <P>VertexLabel<P> where P: OperationParameter {
    pub fn new(op_id: usize, params: P) -> VertexLabel<P> {
        VertexLabel {
            op: Operation::new(op_id, params),
        }
    }

    pub fn new_from_op(op: Operation<P>) -> VertexLabel<P> {
        VertexLabel {
            op: op,
        }
    }
}

#[derive(Clone)]
pub struct CRDT<S: Clone, P: OperationParameter> {
    mutate: fn(&S, &Operation<P>) -> S,
    reconciliation: fn(&Dag<VertexLabel<P>>, &S, fn(&S, &Operation<P>) -> S) -> S,
    pub dag: Dag<VertexLabel<P>>,
    initial_state: S,
    local_id: usize,
    legality: fn(&S, &Operation<P>) -> bool,
}

impl <S: Clone, P: OperationParameter> CRDT<S, P> {
    pub fn new(init: S, mutate: fn(&S, &Operation<P>) -> S, rec: fn(&Dag<VertexLabel<P>>, &S, fn(&S, &Operation<P>) -> S) -> S, leg: fn(&S, &Operation<P>) -> bool) -> CRDT<S, P>
    {
        CRDT { 
            mutate: mutate,
            dag: Dag::new(VertexLabel::new(0, P::default())),    // No process should have id 0
            reconciliation: rec,
            initial_state: init,
            local_id: 1,
            legality: leg
        }
    }

    pub fn append(&mut self, op: Operation<P>, from: u32) -> Result<Vec<VertexId>, IllegalOperationError> {
        let state = self.read();
        if !(self.legality)(&state, &op) {
            return Err(IllegalOperationError)
        }

        let mut heads = self.dag.get_heads();
        let id = VertexId::new(self.local_id, from);
        self.local_id += 1;
        let v = Vertex::new(id, VertexLabel::new_from_op(op));
        self.dag.add_vertex(heads.clone(), v);
        heads.push(id);
        Ok(heads)
    }

    pub fn append_with_causal_context(&mut self, vertex: Vertex<VertexLabel<P>>, causal_context: Vec<VertexId>) -> bool {
        self.dag.add_vertex(causal_context, vertex)
    }

    pub fn read(&self) -> S {
        (self.reconciliation)(&self.dag, &self.initial_state, self.mutate)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::*;
    use crate::crdt::reconciliation_functions::{basic_exploration, fair_reconciliation_no_n};

    #[test]
    fn concurrent_set() {
        // remove wins (1)
        fn add_remove_order(v1: &Vertex<VertexLabel<()>>, v2: &Vertex<VertexLabel<()>>) -> Ordering {
            match (v1.label.op.id, v2.label.op.id) {
                (0, 1) => Ordering::Less,  // add before remove
                (1, 0) => Ordering::Greater, // remove after add
                _ => Ordering::Equal // same operation id
            }
        }

        stable_reconciliation!(Vec<usize>, (), add_remove_order, add_remove_reconciliation);
        // adding concurrency for debugging
        let mut concurrent_set_dag = Dag::new(VertexLabel::<()>::new(0, ()));
        concurrent_set_dag.add_vertex(vec![], Vertex::new(VertexId::new(1, 0), VertexLabel::new(0, ())));  // no concurrent, 0 stays
        concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(2, 0), VertexLabel::new(1, ())));
        concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(3, 0), VertexLabel::new(0, ())));  // concurrent, 1 wins, then 0
        concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(4, 0), VertexLabel::new(0, ())));
        concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(5, 0), VertexLabel::new(1, ())));
        concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(6, 0), VertexLabel::new(0, ()))); 
        concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(7, 0), VertexLabel::new(1, ())));   // 4 concurrent, [1, 1] wins
        

        fn mutate_debug(state: &Vec<usize>, op: &Operation<()>) -> Vec<usize> {
            let mut state = state.clone();
            state.push(op.id);
            state
        }

        let seq = add_remove_reconciliation(&concurrent_set_dag, &vec![], mutate_debug);
        assert_eq!(seq, vec![0, 1, 0, 1, 1, 0, 0]);
    }   

    #[test]
    fn concurrent_bounded_set() {
        // remove wins (1)
        fn add_remove_order(v1: &Vertex<VertexLabel<()>>, v2: &Vertex<VertexLabel<()>>) -> Ordering {
            match (v1.label.op.id, v2.label.op.id) {
                (0, 1) => Ordering::Less,  // add before remove
                (1, 0) => Ordering::Greater, // remove after add
                _ => Ordering::Equal // same operation id
            }
        }

        stable_reconciliation!(Vec<usize>, (), add_remove_order, add_remove_reconciliation);
        fn leg (state: &Vec<usize>, op: &Operation<()>) -> bool {
            state.len() < 4 // bounded set, no more than 2 elements
        }
        fn mutate_debug(state: &Vec<usize>, op: &Operation<()>) -> Vec<usize> {
            let mut state = state.clone();
            state.push(op.id);
            state
        }
        mutate_if_legal!(Vec<usize>, (), mutate_bounded_counter, mutate_debug, leg);

        // adding concurrency for debugging
        let mut concurrent_set_dag = Dag::new(VertexLabel::<()>::new(0, ()));
        concurrent_set_dag.add_vertex(vec![], Vertex::new(VertexId::new(1, 0), VertexLabel::new(0, ())));  // no concurrent, 0 first
        concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(2, 0), VertexLabel::new(1, ())));
        concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(3, 0), VertexLabel::new(0, ())));  // concurrent, 1 wins, then 0
        concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(4, 0), VertexLabel::new(0, ())));
        concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(5, 0), VertexLabel::new(1, ())));
        concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(6, 0), VertexLabel::new(0, ()))); 
        concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(7, 0), VertexLabel::new(1, ())));   // 4 concurrent, [1, 1] wins

        let seq = add_remove_reconciliation(&concurrent_set_dag, &vec![], mutate_bounded_counter);
        assert_eq!(seq, vec![0, 1, 0, 1]);
    }   

    #[test]
    fn fair_set() {
        let onlyconflict = vec![
            vec![true, true],
            vec![true, true]
        ];
        
        let mut fair_concurrent_set_dag = Dag::new(VertexLabel::<()>::new(0, ()));
        fair_concurrent_set_dag.add_vertex(vec![], Vertex::new(VertexId::new(1, 1), VertexLabel::new(0, ())));  // no concurrent, 0 stays
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(1, 1)], Vertex::new(VertexId::new(2, 2), VertexLabel::new(1, ())));
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(1, 1)], Vertex::new(VertexId::new(3, 1), VertexLabel::new(0, ())));  // concurrent, 1 wins (id higher)
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(2, 2), VertexId::new(3, 1)], Vertex::new(VertexId::new(4, 2), VertexLabel::new(1, ()))); //p2 is rollbacked => score of 1
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(2, 2), VertexId::new(3, 1)], Vertex::new(VertexId::new(5, 1), VertexLabel::new(0, ()))); // concurrent, 0 wins (score higher)
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(4, 2), VertexId::new(5, 1)], Vertex::new(VertexId::new(6, 2), VertexLabel::new(1, ())));
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(4, 2), VertexId::new(5, 1)], Vertex::new(VertexId::new(7, 1), VertexLabel::new(0, ()))); 
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(4, 2), VertexId::new(5, 1)], Vertex::new(VertexId::new(8, 3), VertexLabel::new(0, ()))); // 3 concurrent, 1 (p2) wins (score higher)  (p1:1, p3:1) 
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(6, 2), VertexId::new(7, 1), VertexId::new(8, 3)], Vertex::new(VertexId::new(9, 2), VertexLabel::new(0, ())));
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(6, 2), VertexId::new(7, 1), VertexId::new(8, 3)], Vertex::new(VertexId::new(10, 1), VertexLabel::new(0, ()))); 
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(6, 2), VertexId::new(7, 1), VertexId::new(8, 3)], Vertex::new(VertexId::new(11, 3), VertexLabel::new(1, ()))); // 3 concurrent, 1 (p3) wins (p1: 2, p2: 1, p3:0)
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(9, 2), VertexId::new(10, 1), VertexId::new(11, 3)], Vertex::new(VertexId::new(12, 2), VertexLabel::new(0, ())));
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(9, 2), VertexId::new(10, 1), VertexId::new(11, 3)], Vertex::new(VertexId::new(13, 1), VertexLabel::new(1, ()))); 
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(9, 2), VertexId::new(10, 1), VertexId::new(11, 3)], Vertex::new(VertexId::new(14, 3), VertexLabel::new(0, ()))); // 3 concurrent, 1 (p1) wins (p1: 0, p2: 2, p3: 1)
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(12, 2), VertexId::new(13, 1), VertexId::new(14, 3)], Vertex::new(VertexId::new(15, 2), VertexLabel::new(1, ())));
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(12, 2), VertexId::new(13, 1), VertexId::new(14, 3)], Vertex::new(VertexId::new(16, 1), VertexLabel::new(0, ()))); 
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(12, 2), VertexId::new(13, 1), VertexId::new(14, 3)], Vertex::new(VertexId::new(17, 3), VertexLabel::new(0, ()))); // 3 concurrent, 1 (p2) wins

        fn mutate_debug(state: &Vec<usize>, op: &Operation<()>) -> Vec<usize> {
            let mut state = state.clone();
            state.push(op.id);
            state
        }

        let seq = fair_reconciliation_no_n(&fair_concurrent_set_dag, &vec![], mutate_debug);
        assert_eq!(seq, vec![1, 0, 1, 1, 1, 1]);
    }

    #[test]
    fn fair_bounded_set() {
        let onlyconflict = vec![
            vec![true, true],
            vec![true, true]
        ];
        
        let mut fair_concurrent_set_dag = Dag::new(VertexLabel::<()>::new(0, ()));
        fair_concurrent_set_dag.add_vertex(vec![], Vertex::new(VertexId::new(1, 1), VertexLabel::new(0, ())));  // no concurrent, 0 stays
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(1, 1)], Vertex::new(VertexId::new(2, 2), VertexLabel::new(1, ())));
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(1, 1)], Vertex::new(VertexId::new(3, 1), VertexLabel::new(0, ())));  // concurrent, 1 wins (id higher)
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(2, 2), VertexId::new(3, 1)], Vertex::new(VertexId::new(4, 2), VertexLabel::new(1, ()))); //p2 is rollbacked => score of 1
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(2, 2), VertexId::new(3, 1)], Vertex::new(VertexId::new(5, 1), VertexLabel::new(0, ()))); // concurrent, 0 wins (score higher)
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(4, 2), VertexId::new(5, 1)], Vertex::new(VertexId::new(6, 2), VertexLabel::new(1, ())));
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(4, 2), VertexId::new(5, 1)], Vertex::new(VertexId::new(7, 1), VertexLabel::new(0, ()))); 
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(4, 2), VertexId::new(5, 1)], Vertex::new(VertexId::new(8, 3), VertexLabel::new(0, ()))); // 3 concurrent, 1 (p2) wins (score higher)  (p1:1, p3:1) 
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(6, 2), VertexId::new(7, 1), VertexId::new(8, 3)], Vertex::new(VertexId::new(9, 2), VertexLabel::new(0, ())));
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(6, 2), VertexId::new(7, 1), VertexId::new(8, 3)], Vertex::new(VertexId::new(10, 1), VertexLabel::new(0, ()))); 
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(6, 2), VertexId::new(7, 1), VertexId::new(8, 3)], Vertex::new(VertexId::new(11, 3), VertexLabel::new(1, ()))); // 3 concurrent, 1 (p3) wins (p1: 2, p2: 1, p3:0)
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(9, 2), VertexId::new(10, 1), VertexId::new(11, 3)], Vertex::new(VertexId::new(12, 2), VertexLabel::new(0, ())));
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(9, 2), VertexId::new(10, 1), VertexId::new(11, 3)], Vertex::new(VertexId::new(13, 1), VertexLabel::new(1, ()))); 
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(9, 2), VertexId::new(10, 1), VertexId::new(11, 3)], Vertex::new(VertexId::new(14, 3), VertexLabel::new(0, ()))); // 3 concurrent, 1 (p1) wins (p1: 0, p2: 2, p3: 1)
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(12, 2), VertexId::new(13, 1), VertexId::new(14, 3)], Vertex::new(VertexId::new(15, 2), VertexLabel::new(1, ())));
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(12, 2), VertexId::new(13, 1), VertexId::new(14, 3)], Vertex::new(VertexId::new(16, 1), VertexLabel::new(0, ()))); 
        fair_concurrent_set_dag.add_vertex(vec![VertexId::new(12, 2), VertexId::new(13, 1), VertexId::new(14, 3)], Vertex::new(VertexId::new(17, 3), VertexLabel::new(0, ()))); // 3 concurrent, 1 (p2) wins

        fn mutate_debug(state: &Vec<usize>, op: &Operation<()>) -> Vec<usize> {
            let mut state = state.clone();
            if state.len() >= 3 {
                return state; // bounded set, no more than 2 elements
            }

            state.push(op.id);
            state
        }

        let seq = fair_reconciliation_no_n(&fair_concurrent_set_dag, &vec![], mutate_debug);
        assert_eq!(seq, vec![1, 0, 1]);
    }
}