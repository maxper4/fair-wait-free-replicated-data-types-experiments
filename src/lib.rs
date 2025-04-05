mod dag;
pub mod crdt;
mod process;
mod rendering;

use std::vec;

use crdt::{Operation, OperationParameter, VertexLabel, CRDT};
use crdt::reconciliation_functions::{basic_exploration, handling_conflict, fair_reconciliation};
use dag::{Dag, Vertex, VertexId};


pub async fn run() {
   let mut counter = CRDT::new(0, vec![|x, _p| x + 1], basic_exploration);

    counter.apply(Operation::<()>::new(0, ()), 0);
    counter.apply(Operation::<()>::new(0, ()), 0);
    counter.apply(Operation::<()>::new(0, ()), 0);

    let result = counter.read();
    let seq = basic_exploration(&counter.dag).map(|x| x.op.id).collect::<Vec<usize>>();
    println!("Counter {:?} = {}", seq, result);

    let add = |mut x: Vec<i32>, _p: ()| { x.push(4); x };
    let remove = |mut x: Vec<i32>, _p: ()| { x.pop(); x };

    let mut set = CRDT::new(vec![1, 2, 3], vec![add, remove], basic_exploration);
    set.apply(Operation::new(1, ()), 0);
    set.apply(Operation::new(1, ()), 0);
    set.apply(Operation::new(0, ()), 0);

    let result = set.read();
    let seq = basic_exploration(&set.dag).map(|x| x.op.id).collect::<Vec<usize>>();
    println!("Set {:?} = {:?}", seq, result);

    
    // remove wins (1)
    let add_remove_order = vec![
        vec![None, Some(1)],
        vec![Some(1), None]
    ];
    let add_remove_reconciliation = handling_conflict(add_remove_order);
    // adding concurrency for debugging
    let mut concurrent_set_dag = Dag::new(VertexLabel::<()>::new(0, (), 0));
    concurrent_set_dag.add_vertex(vec![], Vertex::new(VertexId::new(1, 0), VertexLabel::new(0, (), 0)));  // no concurrent, 0 stays
    concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(2, 0), VertexLabel::new(1, (), 0)));
    concurrent_set_dag.add_vertex(vec![VertexId::new(1, 0)], Vertex::new(VertexId::new(3, 0), VertexLabel::new(0, (), 0)));  // concurrent, 1 wins
    concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(4, 0), VertexLabel::new(0, (), 0)));
    concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(5, 0), VertexLabel::new(1, (), 0)));
    concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(6, 0), VertexLabel::new(0, (), 0))); 
    concurrent_set_dag.add_vertex(vec![VertexId::new(2, 0), VertexId::new(3, 0)], Vertex::new(VertexId::new(7, 0), VertexLabel::new(1, (), 0)));   // 4 concurrent, [1, 1] wins
    
    let seq = add_remove_reconciliation(&concurrent_set_dag).map(|x| x.op.id).collect::<Vec<usize>>();
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
    let add_remove_fair_reconciliation = fair_reconciliation(onlyconflict);
    let seq = add_remove_fair_reconciliation(&fair_concurrent_set_dag).map(|x| x.op.id).collect::<Vec<usize>>();
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

    let add = |mut x: Vec<i32>, params: ParametersEnum| { 
        let to_add = match params {
            ParametersEnum::Add(v) => v,
            ParametersEnum::Remove(_) => 0
        };
        x.push(to_add);
        x
     };
    let remove = |mut x: Vec<i32>, params: ParametersEnum| { 
        let nb_to_remove = match params {
            ParametersEnum::Add(_) => 0,
            ParametersEnum::Remove(v) => v
        };
        for _ in 0..nb_to_remove {
            x.pop();
        }
        x
     };

    let mut set = CRDT::new(vec![], vec![add, remove], basic_exploration);
    set.apply(Operation::new(0, ParametersEnum::Add(3)), 0);
    set.apply(Operation::new(0, ParametersEnum::Add(4)), 0);
    set.apply(Operation::new(0, ParametersEnum::Add(5)), 0);
    set.apply(Operation::new(1, ParametersEnum::Remove(2)), 0);

    let result = set.read();
    let seq = basic_exploration(&set.dag).map(|x| x.op.id).collect::<Vec<usize>>();
    println!("Set {:?} = {:?}", seq, result);

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

    let add = |mut x: Element, params: ParametersElement| { 
        match params {
            ParametersElement::Add(v) => {x.counter1 += v;},
            ParametersElement::Concat(_) => {},
        };
        x
     };
    let concat = |mut x: Element, params: ParametersElement| { 
        match &params {
            ParametersElement::Add(_) => {},
            ParametersElement::Concat(s) => { x.counter2 = x.counter2 + &s; },
        };
        x
     };

    let mut on_element = CRDT::new(Element::new(), vec![add, concat], basic_exploration);
    on_element.apply(Operation::new(0, ParametersElement::Add(3)), 0);
    on_element.apply(Operation::new(0, ParametersElement::Add(2)), 0);
    on_element.apply(Operation::new(1, ParametersElement::Concat(String::from("hello"))), 0);
    on_element.apply(Operation::new(1, ParametersElement::Concat(String::from(" world"))), 0);

    let result = on_element.read();
    println!("{:?}", result);

}