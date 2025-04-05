use crdt::crdt::{Operation, OperationParameter, CRDT};
use crdt::crdt::reconciliation_functions::{basic_exploration, handling_conflict};

#[test]
fn basic_counter() {
    let mut counter = CRDT::new(0, vec![|x, _p| x + 1], basic_exploration);

    counter.apply(Operation::<()>::new(0, ()), 0);
    counter.apply(Operation::<()>::new(0, ()), 0);
    counter.apply(Operation::<()>::new(0, ()), 0);

    let result = counter.read();
    let seq = basic_exploration(&counter.dag).map(|x| x.op.id).collect::<Vec<usize>>();
    assert_eq!(result, 3);
    assert_eq!(seq, vec![0, 0, 0, 0]);
}

#[test]
fn basic_set() {
    let add = |mut x: Vec<i32>, _p: ()| { x.push(4); x };
    let remove = |mut x: Vec<i32>, _p: ()| { x.pop(); x };

    let mut set = CRDT::new(vec![1, 2, 3], vec![add, remove], basic_exploration);
    set.apply(Operation::new(1, ()), 0);
    set.apply(Operation::new(1, ()), 0);
    set.apply(Operation::new(0, ()), 0);

    let result = set.read();
    let seq = basic_exploration(&set.dag).map(|x| x.op.id).collect::<Vec<usize>>();
    assert_eq!(result, vec![1, 4]);
    assert_eq!(seq, vec![0, 1, 1, 0]);
}

#[test]
fn basic_set_parameters() {
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

    let res = set.read();
    let seq = basic_exploration(&set.dag).map(|x| x.op.id).collect::<Vec<usize>>();
    assert_eq!(seq, vec![0, 0, 0, 0, 1]);
    assert_eq!(res, vec![3]);
}

#[test]
fn basic_different_parameters_types() {
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
    assert_eq!(result.counter1, 5);
    assert_eq!(result.counter2, String::from("hello world"));
}