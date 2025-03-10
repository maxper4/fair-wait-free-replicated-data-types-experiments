mod dag;
mod crdt;
mod process;
mod rendering;

use std::vec::IntoIter;
use std::collections::HashMap;
use std::vec;

use crdt::{Operation, OperationParameter, VertexLabel, CRDT};
use dag::{Dag, Vertex, VertexId};

fn basic_exploration<P>(dag: &Dag<VertexLabel<P>>) -> IntoIter<VertexLabel<P>>  // works when there is no conflict
where P: OperationParameter {
    let mut toexplore = vec![VertexId::new(0, 0)];
    let mut order: Vec<VertexLabel<P>> = vec![];
    let mut children = vec![];

    while toexplore.len() > 0 {
        let head = toexplore.pop().unwrap();
        let v: &Vertex<_> = dag.get_vertex(head).unwrap();
        order.push(v.label.clone());
        for c in dag.get_edges_to_vertex(head) {
            if !children.contains(&c.id) {
                children.push(c.id);
            }
        }
        if toexplore.len() == 0 {
            toexplore.extend(children.clone());
            children.clear();
        }
    }
    order.into_iter()
}

fn handling_conflict<P>(op_order: Vec<Vec<Option<usize>>>) -> impl Fn(&Dag<VertexLabel<P>>) -> IntoIter<VertexLabel<P>>
 where P: OperationParameter { 
    move |dag: &Dag<VertexLabel<_>>| {    // works when there is conflict, returns a function given an order
    let mut toexplore = vec![VertexId::new(0, 0)];
    let mut order: Vec<VertexLabel<_>> = vec![];
    let mut children = vec![];

    while toexplore.len() > 0 {
        let head = toexplore.pop().unwrap();
        for c in dag.get_edges_to_vertex(head) {
            if !children.contains(&c.id) {
                children.push(c.id);
            }
        }

        if toexplore.len() == 0 {   // we explored all vertices at the same level, children is now all vertices at distance k+1
            toexplore.extend(children.clone());
            let mut next_children: Vec<&Vertex<VertexLabel<_>>> = children.clone().into_iter().map(|c| dag.get_vertex(c).unwrap()).collect::<Vec<_>>();
            children.clear();

            if next_children.len() > 1 {
                next_children.sort_by(|x: &&Vertex<VertexLabel<_>>, y: &&Vertex<VertexLabel<_>> | x.label.op.id.cmp(&y.label.op.id));  // what sort should we use?
                
                while next_children.len() > 1 {    // take concurrent operations 2 by 2 and check conflicts
                    let v1: &Vertex<VertexLabel<_>> = next_children.pop().unwrap();
                    let mut conflicted = false;
                    let mut toremove = vec![];

                    for v2 in next_children.iter() {
                        let winner = op_order[v1.label.op.id][v2.label.op.id];
                        match winner {
                            Some(label) => { 
                                if label == v1.label.op.id { 
                                    toremove.push(*v2);
                                }
                                else { 
                                    conflicted = true; 
                                    break; 
                                }
                            }
                            None => { continue; }   // commutes
                        }
                    }
                    
                    // clean loosers
                    for v in toremove.into_iter() {
                        next_children.retain(|x: &&Vertex<VertexLabel<_>>| x.id != v.id);
                    }

                    if !conflicted {    // if no conflict with the whole set of concurrent operations, add in the order
                        order.push(v1.label.clone());
                    }
                }
            } 
            if next_children.len() == 1 {
                order.push(next_children.pop().unwrap().label.clone());
            }
        }
    }
    order.into_iter()
}}

fn fair_reconciliation<P>(op_conflicts: Vec<Vec<bool>>) -> impl Fn(&Dag<VertexLabel<P>>) -> IntoIter<VertexLabel<P>>
where P: OperationParameter { 
    move |dag: &Dag<VertexLabel<_>>| {    // reconciliation is based on process fairness rather than semantically
    let mut toexplore = vec![VertexId::new(0, 0)];
    let mut order: Vec<VertexLabel<_>> = vec![];
    let mut children = vec![];
    let mut scores:HashMap<u32, u32> = HashMap::new();

    while toexplore.len() > 0 {
        let head = toexplore.pop().unwrap();
        for c in dag.get_edges_to_vertex(head) {
            if !children.contains(&c.id) {
                children.push(c.id);
            }
        }
        if toexplore.len() == 0 {   // we explored all vertices at the same level, children is now all vertices at distance k+1
            toexplore.extend(children.clone());
            let mut next_children: Vec<&Vertex<VertexLabel<_>>>  = children.clone().into_iter().map(|c| dag.get_vertex(c).unwrap()).collect::<Vec<_>>();
            children.clear();

            if next_children.len() > 1 {
                next_children.sort_by(|x: &&Vertex<VertexLabel<_>>, y: &&Vertex<VertexLabel<_>> | x.label.op.id.cmp(&y.label.op.id));  // what sort should we use?

                let mut winners: Vec<&Vertex<VertexLabel<_>>>  = vec![];
                let mut loosers: Vec<&Vertex<VertexLabel<_>>> = vec![];
                
                while next_children.len() > 1 {    // take concurrent operations 2 by 2 and check conflicts
                    let v1: &Vertex<VertexLabel<_>> = next_children.pop().unwrap();
                    let mut conflicted = false;
                    let mut toremove = vec![];

                    for v2 in next_children.iter() {
                        let conflict = op_conflicts[v1.label.op.id][v2.label.op.id];
                        if conflict {
                            let p1 = v1.label.process_id;
                            let p2 = v2.label.process_id;
                            let score_p1 = *scores.get(&p1).unwrap_or(&0);
                            let score_p2 = *scores.get(&p2).unwrap_or(&0);

                            if score_p1 > score_p2 || (score_p1 == score_p2 && p1 >= p2) {
                                toremove.push(*v2);
                            } else {
                                conflicted = true;
                            }
                        }
                    }
                    
                    // clean loosers
                    for v in toremove.into_iter() {
                        next_children.retain(|x: &&Vertex<VertexLabel<_>>| x.id != v.id);
                        loosers.push(v);
                    }

                    if conflicted {
                        loosers.push(v1);
                    } else {    // if no conflict with the whole set of concurrent operations, add in the order
                        winners.push(v1);
                    }
                }
                if next_children.len() == 1 {
                    let v: &Vertex<VertexLabel<_>> = next_children.pop().unwrap();
                    winners.push(v);
                }

                for v in winners {
                    scores.insert(v.label.process_id, 0);
                    order.push(v.label.clone());
                }

                for v in loosers {
                    let old_score = *scores.get(&v.label.process_id).unwrap_or(&0);
                    scores.insert(v.label.process_id, old_score + 1);
                }
            }
            if next_children.len() == 1 {
                let v: &Vertex<VertexLabel<_>> = next_children.pop().unwrap();
                order.push(v.label.clone());
                scores.insert(v.label.process_id, 0);
            }
        }
    }
    order.into_iter()
}}

#[tokio::main]
async fn main() {
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