mod dag;
mod crdt;

use crdt::CRDT;
use dag::Dag;

fn main() {

    let basic_exploration = |dag: &Dag<usize>| {    // works when there is no conflict
        let mut toexplore = vec![dag.get_root()];
        let mut order = vec![];
        while toexplore.len() > 0 {
            let head = toexplore.pop().unwrap();
            order.push(head.label);
            toexplore.extend(dag.get_edges_to_vertex(head.id as usize).into_iter());
        }
        order.into_iter()
    };

    let mut counter = CRDT::new(0, vec![|x| x + 1], basic_exploration);

    counter.apply(0);
    counter.apply(0);
    counter.apply(0);
    
    let result = counter.read();
    let seq = basic_exploration(&counter.dag).collect::<Vec<usize>>();
    println!("Counter {:?} = {}", seq, result);

    let add = |mut x: Vec<i32>| { x.push(4); x };
    let remove = |mut x: Vec<i32>| { x.pop(); x };

    let mut set = CRDT::new(vec![1, 2, 3], vec![add, remove], basic_exploration);
    set.apply(1);
    set.apply(1);
    set.apply(0);

    let result = set.read();
    let seq = basic_exploration(&set.dag).collect::<Vec<usize>>();
    println!("Set {:?} = {:?}", seq, result);
}