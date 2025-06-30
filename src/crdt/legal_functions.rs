use crate::crdt::{Operation, OperationParameter, VertexLabel, CRDT};
use std::fmt;

pub fn total<P, S>(state: &S, op: &Operation<P>) -> bool where P: OperationParameter, S: Clone {
    true
}

#[macro_export]
macro_rules! mutate_if_legal
{
    ($S:ty,$P:ty,$name:ident, $mutate:ident, $leg:ident) => {
        fn $name(state: &$S, op: &Operation<$P>) -> $S {
            if $leg(state, op) {
                $mutate(state, op)
            } else {
                state.clone()
            }
        }
    };
}

#[derive(Debug, Clone)]
pub struct IllegalOperationError;

impl fmt::Display for IllegalOperationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "illegal operation")
    }
}