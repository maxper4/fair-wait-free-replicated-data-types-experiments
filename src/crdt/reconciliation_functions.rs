use crate::crdt::{Operation, OperationParameter, VertexLabel, CRDT};
use crate::dag::{Dag, Vertex, VertexId};

use std::hash::Hash;
use std::vec::IntoIter;
use std::collections::HashMap;

pub fn basic_exploration<P, S>(dag: &Dag<VertexLabel<P>>, initial_state: &S, mutate: fn(&S, &Operation<P>) -> S) -> S  // works when there is no conflict
where P: OperationParameter, S: Clone {
    let mut state = initial_state.clone();
    let mut all = dag.get_all_ids().iter().map(|x| dag.get_vertex(x).unwrap()).collect::<Vec<_>>();
    all.sort_by(|x, y| x.distance.cmp(&y.distance)); // sort all vertices by distance from the root
    for v in all {
        state = mutate(&state, &v.label.op);
    }
    state
}

#[macro_export]
macro_rules! order_based_reconciliation {
($S:ty,$P:ty,$op_order:ident,$name:ident) => {
    fn $name(dag: &Dag<VertexLabel<$P>>, initial_state: &$S, mutate: fn(&$S, &Operation<$P>) -> $S) -> $S {
        let mut state = initial_state.clone();
        let mut all = dag.get_all_ids().iter().map(|x| dag.get_vertex(x).unwrap()).collect::<Vec<_>>();
        all.sort_by(|x, y| x.distance.cmp(&y.distance)); // sort all vertices by distance from the root

        for i in 1..(dag.length+1) {
            let mut concurrent = all.iter().filter(|v| v.distance == i).collect::<Vec<_>>();
            concurrent.sort_by(|x, y| $op_order(*y,*x)); // sort concurrent vertices by operation order
            for v in concurrent {
                state = mutate(&state, &v.label.op);
            }
        }
        
        state
    }
};
}

// reconciliation based on process fairness rather than semantically
pub fn fair_reconciliation_no_n<P, S>(dag: &Dag<VertexLabel<P>>, initial_state: &S, mutate: fn(&S, &Operation<P>) -> S) -> S 
where P: OperationParameter, S: Clone {
    let mut state = initial_state.clone();
    let mut counter: HashMap<u32, u32> = HashMap::new();
    let mut candidates: Vec<VertexId> = vec![VertexId::new(0, 0)]; // start with the root vertex
    let all = dag.get_all_ids();
    let mut explored: HashMap<VertexId, bool> = HashMap::new();
    for v in all.clone() {
        explored.insert(*v, false);
    }

    while candidates.len() > 0 {
        candidates.sort_by(|x, y| x.cmp(y)); // sort candidates by id (for instance, to have a deterministic order)
        let current = candidates.remove(0);
        let past = dag.past(&current, &explored); // past should be sorted as a reverse BFS
        for p in past {
            state = mutate(&state, &dag.get_vertex(&p).unwrap().label.op);
            explored.insert(p, true);
        }
        explored.insert(current, true);
        state = mutate(&state, &dag.get_vertex(&current).unwrap().label.op);
        counter.insert(dag.get_vertex(&current).unwrap().id.process_id, counter.get(&dag.get_vertex(&current).unwrap().id.process_id).unwrap_or(&0) + 1);

        let mut alive = dag.future(&current).iter().map(|v| v.process_id).collect::<Vec<_>>();
        alive.sort();
        alive.dedup();
        if alive.len() == 0 {
            break; // no more future vertices
        }
        let min = alive.iter().map(|p| *counter.get(p).unwrap_or(&0)).min().unwrap_or(0);
        let starving = alive.iter().filter(|p| *counter.get(p).unwrap_or(&0) == min).collect::<Vec<_>>();
        candidates = vec![*dag.first_from_processes(&current, &starving)];
    }

    let mut remaining = all.iter().filter(|x| !explored[x]).collect::<Vec<_>>();
    remaining.sort_by(|x, y| dag.get_vertex(x).unwrap().distance.cmp(&dag.get_vertex(y).unwrap().distance)); // sort remaining vertices by distance from the root
    for v in remaining {
        state = mutate(&state, &dag.get_vertex(v).unwrap().label.op);
    }

    state
}

#[macro_export]
macro_rules! fair_reconciliation_n {
($n:ident) => {
    pub fn fair_reconciliation_n<P, S>(dag: &Dag<VertexLabel<P>>, initial_state: &S, mutate: fn(&S, &Operation<P>) -> S) -> S 
where P: OperationParameter, S: Clone {
    use std::collections::HashMap; // Ensure HashMap is in scope
    let n = $n();
    let mut state = initial_state.clone();
    let mut candidates: Vec<VertexId> = vec![VertexId::new(0, 0)]; // start with the root vertex
    let all = dag.get_all_ids();
    let mut explored: HashMap<VertexId, bool> = HashMap::new();
    for v in all.clone() {
        explored.insert(*v, false);
    }

    while candidates.len() > 0 {
        candidates.sort_by(|x, y| x.cmp(y)); // sort candidates by id (for instance, to have a deterministic order)
        let current = candidates.remove(0);
        let past = dag.past(&current, &explored); // past should be sorted as a reverse BFS
        for p in past {
            state = mutate(&state, &dag.get_vertex(&p).unwrap().label.op);
            explored.insert(p, true);
        }
        explored.insert(current, true);
        state = mutate(&state, &dag.get_vertex(&current).unwrap().label.op);

        let alive = dag.processes_in_future(&current, n);
        if alive.len() == 0 {
            break; // no more future vertices
        }
        let i = current.process_id;
        let mut next = 0;
        for j in 1..n {
            if alive.contains(&((i + j) % n)) {
                next = (i + j) % n;
                break;
            }
        }

        candidates = vec![*dag.first_from_processes(&current, &vec![&next])];
    }

    let mut remaining = all.iter().filter(|x| !explored[x]).collect::<Vec<_>>();
    remaining.sort_by(|x, y| dag.get_vertex(x).unwrap().distance.cmp(&dag.get_vertex(y).unwrap().distance)); // sort remaining vertices by distance from the root
    for v in remaining {
        state = mutate(&state, &dag.get_vertex(v).unwrap().label.op);
    }

    state
}
};
}