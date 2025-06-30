mod dag;
pub mod crdt;
mod process;
mod rendering;

use std::cmp::Ordering;
use std::vec;

use crdt::{Operation, OperationParameter, VertexLabel, CRDT};
use crdt::reconciliation_functions::{basic_exploration, fair_reconciliation_no_n};
use crdt::legal_functions::total;
use dag::{Dag, Vertex, VertexId};


pub async fn run() {
    fn mutate_counter(state: &u32, op: &Operation<()>) -> u32 {
        *state + 1
    }
    
   let mut counter = CRDT::new(0, mutate_counter, basic_exploration, total);

    counter.append(Operation::<()>::new(0, ()), 0);
    counter.append(Operation::<()>::new(0, ()), 0);
    counter.append(Operation::<()>::new(0, ()), 0);

    let result = counter.read();
    println!("Counter = {:?}", result);

    fn leg(state: &u32, op: &Operation<()>) -> bool {
        if *state < 3 {
            true
        } else {
            false
        }
    }

    fn mutate_clamped_counter(state: &u32, op: &Operation<()>) -> u32 {
        if leg(state, op) {
            *state + 1
        } else {
            *state
        }
    }
    let mut clamped_counter = CRDT::new(0, mutate_clamped_counter, basic_exploration, leg);

    clamped_counter.append(Operation::<()>::new(0, ()), 0);
    clamped_counter.append(Operation::<()>::new(0, ()), 0);
    clamped_counter.append(Operation::<()>::new(0, ()), 0);
    clamped_counter.append(Operation::<()>::new(0, ()), 0);

    let result = clamped_counter.read();
    println!("Clamped counter = {:?}", result);

    fn mutate_set(state: &Vec<i32>, op: &Operation<()>) -> Vec<i32> {
        let mut state = state.clone();
        match op.id {
            0 => {
                state.push(4); state
            },
            1  => {
                state.pop(); state
            }, 
            _ => state
        }
    }

    let mut set = CRDT::new(vec![1, 2, 3], mutate_set, basic_exploration, total);
    set.append(Operation::new(1, ()), 0);
    set.append(Operation::new(1, ()), 0);
    set.append(Operation::new(0, ()), 0);

    let result = set.read();
    println!("Set = {:?}", result);

    
    // remove wins (1)
    fn add_remove_order(v1: &Vertex<VertexLabel<()>>, v2: &Vertex<VertexLabel<()>>) -> Ordering {
        match (v1.label.op.id, v2.label.op.id) {
            (0, 1) => Ordering::Less,  // add before remove
            (1, 0) => Ordering::Greater, // remove after add
            _ => Ordering::Equal // same operation id
        }
    }

    order_based_reconciliation!(Vec<usize>, (), add_remove_order, add_remove_reconciliation);
    // adding concurrency for debugging
    let mut concurrent_set_dag = Dag::new(VertexLabel::<()>::new(0, (), 0));
    concurrent_set_dag.add_vertex(vec![], Vertex::new(VertexId::new(1, 0), VertexLabel::new(0, (), 0)));  // no concurrent, 0 stays
    concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(2, 0), VertexLabel::new(1, (), 0)));
    concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(3, 0), VertexLabel::new(0, (), 0)));  // concurrent, 1 wins
    concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(4, 0), VertexLabel::new(0, (), 0)));
    concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(5, 0), VertexLabel::new(1, (), 0)));
    concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(6, 0), VertexLabel::new(0, (), 0))); 
    concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(7, 0), VertexLabel::new(1, (), 0)));   // 4 concurrent, [1, 1] wins
    
    fn mutate_debug(state: &Vec<usize>, op: &Operation<()>) -> Vec<usize> {
        let mut state = state.clone();
        state.push(op.id);
        state
    }

    let seq = add_remove_reconciliation(&concurrent_set_dag, &vec![], mutate_debug);
    println!("Concurrent Set {:?}", seq);

    let onlyconflict = vec![
        vec![true, true],
        vec![true, true]
    ];
    // adding concurrency for debugging
    let mut fair_concurrent_set_dag = Dag::new(VertexLabel::<()>::new(0, (), 0));
    fair_concurrent_set_dag.add_vertex(vec![], Vertex::new(VertexId::new(1, 0), VertexLabel::new(0, (), 1)));  // no concurrent, 0 stays
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(2, 0), VertexLabel::new(1, (), 2)));
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(3, 0), VertexLabel::new(0, (), 1)));  // concurrent, 1 wins (id higher)
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(4, 0), VertexLabel::new(1, (), 2))); //p2 is rollbacked => score of 1
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(5, 0), VertexLabel::new(0, (), 1))); // concurrent, 0 wins (score higher)
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(4, 0), VertexId::new(5, 0)], Vertex::new(VertexId::new(6, 0), VertexLabel::new(1, (), 2)));
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(4, 0), VertexId::new(5, 0)], Vertex::new(VertexId::new(7, 0), VertexLabel::new(0, (), 1))); 
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(4, 0), VertexId::new(5, 0)], Vertex::new(VertexId::new(8, 0), VertexLabel::new(0, (), 3))); // 3 concurrent, 1 (p2) wins (score higher)  (p1:1, p3:1) 
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(6, 0), VertexId::new(7, 0), VertexId::new(8, 0)], Vertex::new(VertexId::new(9, 0), VertexLabel::new(0, (), 2)));
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(6, 0), VertexId::new(7, 0), VertexId::new(8, 0)], Vertex::new(VertexId::new(10, 0), VertexLabel::new(0, (), 1))); 
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(6, 0), VertexId::new(7, 0), VertexId::new(8, 0)], Vertex::new(VertexId::new(11, 0), VertexLabel::new(1, (), 3))); // 3 concurrent, 1 (p3) wins (p1: 2, p2: 1, p3:0)
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(9, 0), VertexId::new(10, 0), VertexId::new(11, 0)], Vertex::new(VertexId::new(12, 0), VertexLabel::new(0, (), 2)));
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(9, 0), VertexId::new(10, 0), VertexId::new(11, 0)], Vertex::new(VertexId::new(13, 0), VertexLabel::new(1, (), 1))); 
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(9, 0), VertexId::new(10, 0), VertexId::new(11, 0)], Vertex::new(VertexId::new(14, 0), VertexLabel::new(0, (), 3))); // 3 concurrent, 1 (p1) wins (p1: 0, p2: 2, p3: 1)
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(12, 0), VertexId::new(13, 0), VertexId::new(14, 0)], Vertex::new(VertexId::new(15, 0), VertexLabel::new(1, (), 2)));
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(12, 0), VertexId::new(13, 0), VertexId::new(14, 0)], Vertex::new(VertexId::new(16, 0), VertexLabel::new(0, (), 1))); 
    fair_concurrent_set_dag.add_vertex(vec![VertexId::new(12, 0), VertexId::new(13, 0), VertexId::new(14, 0)], Vertex::new(VertexId::new(17, 0), VertexLabel::new(0, (), 3))); // 3 concurrent, 1 (p2) wins

    let seq = fair_reconciliation_no_n(&fair_concurrent_set_dag, &vec![], mutate_debug);
    println!("Fair concurrent Set {:?}", seq);  // should be [0, 1, 0, 1, 1, 1, 1]


    #[derive(Clone, PartialEq, Eq)]
    enum ParametersEnum {
        Add(i32),
        Remove(usize)
    }

    impl Default for ParametersEnum {
        fn default() -> Self {
            ParametersEnum::Add(0)
        }
    }
    impl OperationParameter for ParametersEnum {}

    fn mutate_set_params(state: &Vec<i32>, op: &Operation<ParametersEnum>) -> Vec<i32> {
        let mut state = state.clone();
        match op.id {
            0 => {
                let to_add = match op.params {
                    ParametersEnum::Add(v) => v,
                    ParametersEnum::Remove(_) => 0
                };
                state.push(to_add);
            },
            1  => {
                let nb_to_remove = match op.params {
                    ParametersEnum::Add(_) => 0,
                    ParametersEnum::Remove(v) => v
                };
                for _ in 0..nb_to_remove {
                    state.pop();
                }
            }, 
            _ => ()
        }
        state
    }


    let mut set = CRDT::new(vec![], mutate_set_params, basic_exploration, total);
    set.append(Operation::new(0, ParametersEnum::Add(3)), 0);
    set.append(Operation::new(0, ParametersEnum::Add(4)), 0);
    set.append(Operation::new(0, ParametersEnum::Add(5)), 0);
    set.append(Operation::new(1, ParametersEnum::Remove(2)), 0);

    let result = set.read();
    println!("Set = {:?}", result);

    #[derive(Clone, Debug)]
    struct Element {
        counter1: i32,
        counter2: String,
    }

    impl Element {
        fn new() -> Element {
            Element {
                counter1: 0,
                counter2: String::from(""),
            }
        }
    }

    #[derive(Clone, PartialEq, Eq)]
    enum ParametersElement {
        Add(i32),
        Concat(String),
    }

    impl Default for ParametersElement {
        fn default() -> Self {
            ParametersElement::Add(0)
        }
    }

    impl OperationParameter for ParametersElement {}

    fn mutate_set_elements(state: &Element, op: &Operation<ParametersElement>) -> Element {
        let mut state = state.clone();
        match op.id {
            0 => {
                match op.params {
                    ParametersElement::Add(v) => {state.counter1 += v; },
                    _ => (),
                };
            },
            1  => {
                match &op.params {
                    ParametersElement::Concat(s) => { state.counter2 = state.counter2 + &s; },
                    _ => (),
                };
            }, 
            _ => ()
        }
        state
    }

    let mut on_element = CRDT::new(Element::new(), mutate_set_elements, basic_exploration, total);
    on_element.append(Operation::new(0, ParametersElement::Add(3)), 0);
    on_element.append(Operation::new(0, ParametersElement::Add(2)), 0);
    on_element.append(Operation::new(1, ParametersElement::Concat(String::from("hello"))), 0);
    on_element.append(Operation::new(1, ParametersElement::Concat(String::from(" world"))), 0);

    let result = on_element.read();
    println!("{:?}", result);
}