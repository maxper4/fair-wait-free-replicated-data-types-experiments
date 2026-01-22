use crate::crdt::{Operation, OperationParameter, VertexLabel, CRDT};
use crate::dag::{Dag, Vertex, VertexId};

use std::hash::Hash;
use std::collections::HashMap;
use std::fmt::Debug;

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
macro_rules! crdt_reconciliation {
($S:ty,$P:ty,$op_order:ident,$name:ident) => {
    fn $name(dag: &Dag<VertexLabel<$P>>, initial_state: &$S, mutate: fn(&$S, &Operation<$P>) -> $S) -> $S {
        let mut state = initial_state.clone();
        let mut all = dag.get_all_ids().iter().map(|x| dag.get_vertex(x).unwrap()).collect::<Vec<_>>();
        all.sort_by(|x, y| $op_order(*y,*x)); // sort all vertices by operation order ("op-wins" arbitration)
        
        for v in all {
            state = mutate(&state, &v.label.op);
        }
        
        state
    }
};
}

// reconciliation based on the distance from the root and operation order $order_concurrent among concurrent operations)
#[macro_export]
macro_rules! stable_reconciliation {
($S:ty,$P:ty,$order_concurrent:ident,$name:ident) => {
    fn $name(dag: &Dag<VertexLabel<$P>>, initial_state: &$S, mutate: fn(&$S, &Operation<$P>) -> $S) -> $S {
        let mut state = initial_state.clone();
        let mut all = dag.get_all_ids().iter().map(|x| dag.get_vertex(x).unwrap()).collect::<Vec<_>>();
        all.sort_by(|x, y| x.distance.cmp(&y.distance)); // sort all vertices by distance from the root

        for i in 1..(dag.length+1) {
            let mut concurrent = all.iter().filter(|v| v.distance == i).collect::<Vec<_>>();
            concurrent.sort_by(|x, y| $order_concurrent(*y,*x)); // sort concurrent vertices by operation order
            for v in concurrent {
                state = mutate(&state, &v.label.op);
            }
        }
        
        state
    }
};
}

// reconciliation based on process fairness (ffair)
pub fn fair_reconciliation_no_n<P, S>(dag: &Dag<VertexLabel<P>>, initial_state: &S, mutate: fn(&S, &Operation<P>) -> S) -> S 
where P: OperationParameter, S: Clone+Debug+Hash {
    let mut state = initial_state.clone();
    let mut counter: HashMap<u32, u32> = HashMap::new();
    let mut candidates: Vec<VertexId> = dag.get_edges_to_vertex(&dag.get_root().id); // start with the root vertex
    // let all = dag.get_all_ids();
    let mut explored: HashMap<VertexId, bool> = HashMap::new();
    // for v in all.clone() {
    //     explored.insert(*v, false);
    // }
    explored.insert(dag.get_root().id, true);
    let mut z = 0;

    while candidates.len() > 0 {
        z += 1;
        //candidates.sort_by(|x, y| (*x).cmp(y)); // sort candidates by id (for instance, to have a deterministic order)
        let current = candidates.remove(0);

        let mut heads = dag.get_edges_from_vertex(&current);     // reconstitute current's initial context
        heads.retain(|x| !explored.get(x).unwrap_or(&false));
        //heads.sort_by(|x, y| (*x).cmp(y)); 
        let past = dag.sorted_past(heads.iter().map(|x| x).collect(), &explored);

        for p in past {
            state = mutate(&state, &dag.get_vertex(&p).unwrap().label.op);
            explored.insert(p, true);
        }

        //println!("Z: {}, Leader: {:?}, with {}, before: {:?}", z, current.process_id, current, state);

        explored.insert(current, true);
        state = mutate(&state, &dag.get_vertex(&current).unwrap().label.op);
        counter.insert(dag.get_vertex(&current).unwrap().id.process_id, counter.get(&dag.get_vertex(&current).unwrap().id.process_id).unwrap_or(&0) + 1);

        let alive = dag.processes_in_future_no_n(&current);
        if alive.len() == 0 {
            break; // no more future vertices
        }
        let min = alive.iter().map(|p| *counter.get(p).unwrap_or(&0)).min().unwrap_or(0);
        let starving = alive.iter().filter(|p| *counter.get(p).unwrap_or(&0) == min).collect::<Vec<_>>();
        candidates = vec![dag.first_from_processes(&current, &starving)];
    }

    // let remaining = all.iter().filter(|x| !explored[*x]).collect::<Vec<_>>();
    // println!("Remaining length: {}/{}/{}", remaining.len(), all.len(), z);

    let binding = dag.get_heads();
    let mut heads = binding.iter().map(|x| x).collect::<Vec<_>>();
    heads.retain(|x| !explored.get(x).unwrap_or(&false));
    //heads.sort_by(|x, y| (**x).cmp(*y));  
    
    let remaining = dag.sorted_past(heads, &explored);
    for v in remaining {
        state = mutate(&state, &dag.get_vertex(&v).unwrap().label.op);
    }
    
    state
}

// fair reconciliation when the number of processes is known, not used
#[macro_export]
macro_rules! fair_reconciliation_n {
($n:expr) => {
    pub fn fair_reconciliation_n<P, S>(dag: &Dag<VertexLabel<P>>, initial_state: &S, mutate: fn(&S, &Operation<P>) -> S) -> S 
where P: OperationParameter, S: Clone {
    use std::collections::HashMap; // Ensure HashMap is in scope
    use crate::dag::{Dag, Vertex, VertexId};
    let n = $n;
    let mut state = initial_state.clone();
    let mut candidates: Vec<VertexId> = vec![VertexId::new(0, 0)]; // start with the root vertex
    let all = dag.get_all_ids();
    let mut explored: HashMap<VertexId, bool> = HashMap::new();
    for v in all.clone() {
        explored.insert(*v, false);
    }

    while candidates.len() > 0 {
        candidates.sort_by(|x, y| (**x).cmp(*y)); // sort candidates by id (for instance, to have a deterministic order)
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