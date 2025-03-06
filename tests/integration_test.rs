use std::vec::IntoIter;

use crdt::crdt::legal_functions::total;
use crdt::crdt::{Operation, OperationParameter, CRDT};
use crdt::crdt::reconciliation_functions::{basic_exploration, fair_reconciliation_no_n};
use crdt::mutate_if_legal;
use serde::{Deserialize, Serialize};

#[test]
fn basic_counter() {
    fn mutate_counter(state: &u32, op: &Operation<()>) -> u32 {
        *state + 1
    }
    
   let mut counter = CRDT::new(0, mutate_counter, basic_exploration, total);

    counter.append(Operation::<()>::new(0, ()), 0);
    counter.append(Operation::<()>::new(0, ()), 0);
    counter.append(Operation::<()>::new(0, ()), 0);

    let result = counter.read();

    fn mutate_debug(state: &Vec<usize>, op: &Operation<()>) -> Vec<usize> {
        let mut state = state.clone();
        state.push(op.id);
        state
    }

    let seq = basic_exploration(&counter.dag, &vec![], mutate_debug);

    assert_eq!(result, 3);
    assert_eq!(seq, vec![0, 0, 0]);
}

#[test]
fn basic_set() {
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

    fn mutate_debug(state: &Vec<usize>, op: &Operation<()>) -> Vec<usize> {
        let mut state = state.clone();
        state.push(op.id);
        state
    }

    let seq = basic_exploration(&set.dag, &vec![], mutate_debug);
    assert_eq!(result, vec![1, 4]);
    assert_eq!(seq, vec![1, 1, 0]);
}

#[test]
fn basic_set_parameters() {
    #[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    let res = set.read();

    fn mutate_debug(state: &Vec<usize>, op: &Operation<ParametersEnum>) -> Vec<usize> {
        let mut state = state.clone();
        state.push(op.id);
        state
    }

    let seq = basic_exploration(&set.dag, &vec![], mutate_debug);

    assert_eq!(seq, vec![0, 0, 0, 1]);
    assert_eq!(res, vec![3]);
}

#[test]
fn basic_different_parameters_types() {
    #[derive(Clone, Debug, Serialize, Deserialize)]
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

    #[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    assert_eq!(result.counter1, 5);
    assert_eq!(result.counter2, String::from("hello world"));
}

#[test]
fn bounded_counter() {
    fn leg(state: &u32, op: &Operation<()>) -> bool {
        *state < 2
    }

    fn increment(state: &u32, op: &Operation<()>) -> u32 {
        *state + 1
    }

    mutate_if_legal!(u32, (), mutate_counter, increment, leg);

    let mut bounded_counter = CRDT::new(0, mutate_counter, basic_exploration, leg);

    bounded_counter.append(Operation::<()>::new(0, ()), 0);
    bounded_counter.append(Operation::<()>::new(0, ()), 0);
    bounded_counter.append(Operation::<()>::new(0, ()), 0);
    bounded_counter.append(Operation::<()>::new(0, ()), 0);
    bounded_counter.append(Operation::<()>::new(0, ()), 0);

    let result = bounded_counter.read();
    assert_eq!(result, 2);
}

#[test]
fn up_down_counter() {
    fn counter(state: &i32, op: &Operation<()>) -> i32 {
        match op.id {
            0 => *state + 1, // increment
            1 => *state - 1, // decrement
            _ => *state,     // no change for other operations
        }
    }
    mutate_if_legal!(i32, (), mutate_counter, counter, total);

    let mut counter = CRDT::new(0, mutate_counter, basic_exploration, total);
    counter.append(Operation::<()>::new(0, ()), 0); // increment
    counter.append(Operation::<()>::new(0, ()), 0); // increment
    assert_eq!(counter.read(), 2);

    counter.append(Operation::<()>::new(1, ()), 0); // decrement
    counter.append(Operation::<()>::new(1, ()), 0); // decrement
    counter.append(Operation::<()>::new(1, ()), 0); // decrement
    assert_eq!(counter.read(), -1);
}