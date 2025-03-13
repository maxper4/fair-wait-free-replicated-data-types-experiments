use std::{collections::HashMap, fmt::{self}};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VertexId {
    pub local_id: usize,
    pub process_id: u32
}

impl VertexId {
    pub fn new(local_id: usize, process_id: u32) -> VertexId {
        VertexId {
            local_id: local_id,
            process_id: process_id
        }
    }
}

impl fmt::Display for VertexId {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}", self.process_id, self.local_id)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Vertex<T> {
    pub id: VertexId,
    pub label: T,
}

impl <T>Vertex<T> {
    pub fn new(id: VertexId, l: T) -> Vertex<T> {
        Vertex { id, label: l }
    }
}

// #[derive(Debug, Clone, Copy)]
// struct Edge<'a, T> {
//     pub from: &'a Vertex<T>,
//     pub to: &'a Vertex<T>,
// }

// impl<'a,T> Edge<'a, T> {
//     fn new(from: &'a Vertex<T>, to: &'a Vertex<T>) -> Edge<'a, T> {
//         Edge { from, to }
//     }
// }

#[derive(Debug, Clone)]
pub struct Dag<T> {
    vertices: Vec<Vertex<T>>,
    edges: HashMap<VertexId, Vec<VertexId>>, // from -> to
}

impl<T> Dag<T> {
    pub fn new(init: T) -> Dag< T> {
        Dag {
            vertices: vec![Vertex::new(VertexId::new(0, 0), init)],
            edges: HashMap::new(),
        }
    }

    pub fn add_vertex(&mut self, parents: Vec<VertexId>, v: Vertex<T>) {
        self.vertices.push(v);
        let v = &self.vertices[self.vertices.len() - 1];
        let parents_len = parents.len();
        for v2 in parents { // TODO: check if the parent exists
            //let e = Edge::new(v, &self.vertices[v2 as usize]);
            let parents = self.edges.get_mut(&v.id);
            match parents {
                Some(p) => {
                    p.push(v2);
                },
                None => {
                    self.edges.insert(v.id, vec![v2]);
                }
                
            }
        }
        if parents_len == 0 {  // if no parent just add an edge to the root
            //let e = Edge::new(v, &self.vertices[0]);
            let parents = self.edges.get_mut(&v.id);
            match parents {
                Some(p) => {
                    p.push(VertexId::new(0, 0));
                },
                None => {
                    self.edges.insert(v.id, vec![VertexId::new(0, 0)]);
                }
                
            }
        }
    }

    pub fn get_root(&self) -> &Vertex<T> {
        &self.vertices[0]
    }

    pub fn get_vertex(&self, id: VertexId) -> Option<&Vertex<T>> {
        for v in &self.vertices {
            if v.id == id {
                return Some(v);
            }
        }

        None
    }

    pub fn get_edges_to_vertex(&self, id: VertexId) -> Vec<&Vertex<T>> {
        let mut edges = vec![];
        for v in &self.vertices {
            if let Some(parents) = self.edges.get(&(v.id)) {
                if parents.contains(&id) {
                    edges.push(v);
                }
            }
        }

        edges
    }

    pub fn get_heads(&self) -> Vec<VertexId> {
        let mut heads = vec![];
        for v in &self.vertices {
             if self.get_edges_to_vertex(v.id).len() == 0 {
                heads.push(v.id);
             }
        }

        heads
    }
}
