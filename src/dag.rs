use std::{collections::HashMap, fmt::{self}};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
pub struct Vertex<T> where T: Clone{
    pub id: VertexId,
    pub label: T,
    pub distance: u32
}

impl <T>Vertex<T> where T: Clone {
    pub fn new(id: VertexId, l: T) -> Vertex<T> {
        Vertex { id, label: l, distance: 0 }
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
pub struct Dag<T> where T: Clone {
    vertices: Vec<Vertex<T>>,
    edges: HashMap<VertexId, Vec<VertexId>>, // from -> to
    pub length: u32
}

impl<T> Dag<T> where T: Clone {
    pub fn new(init: T) -> Dag< T> {
        Dag {
            vertices: vec![Vertex::new(VertexId::new(0, 0), init)],
            edges: HashMap::new(),
            length: 1
        }
    }

    pub fn add_vertex(&mut self, parents: Vec<VertexId>, mut v: Vertex<T>) {// TODO: delay until parents exist
        if v.distance == 0 {
            v.distance = parents.iter().map(|p| self.get_vertex(p).unwrap_or_else(|| self.get_root()).distance).max().unwrap_or(0) + 1;
        }

        if self.length < v.distance {
            self.length = v.distance;
        }

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

    pub fn get_vertex(&self, id: &VertexId) -> Option<&Vertex<T>> {
        for v in &self.vertices {
            if v.id == *id {
                return Some(v);
            }
        }

        None
    }

    pub fn get_edges_to_vertex(&self, id: &VertexId) -> Vec<&VertexId> {
        let mut edges = vec![];
        for v in &self.vertices {
            if let Some(parents) = self.edges.get(&(v.id)) {
                if parents.contains(&id) {
                    edges.push(&v.id);
                }
            }
        }

        edges
    }

    pub fn get_edges_from_vertex(&self, id: &VertexId) -> Vec<VertexId> {
        self.edges.get(id).unwrap_or(&Vec::<VertexId>::new()).clone()
    }

    pub fn get_heads(&self) -> Vec<VertexId> {
        let mut heads = vec![];
        for v in &self.vertices {
             if self.get_edges_to_vertex(&v.id).len() == 0 {
                heads.push(v.id);
             }
        }

        heads
    }

    pub fn get_all_ids(&self) -> Vec<&VertexId> {
        self.vertices.iter().map(|v| &v.id).collect()
    }

    pub fn past(&self, v: &VertexId, explored: &HashMap<VertexId, bool>) -> Vec<VertexId> {
        let mut past = vec![];
        let mut toexplore = self.get_edges_from_vertex(v);
        let mut seen = explored.clone();

        while toexplore.len() > 0 {
            let head = toexplore.remove(0); // BFS
            if !seen.contains_key(&head) {
                past.push(head);
                for parent in self.get_edges_from_vertex(&head) {
                    if !seen.contains_key(&parent) {
                        toexplore.push(parent);
                        seen.insert(parent, true);
                    }
                }
            }
        }
        past.reverse();
        past
    }

    pub fn future(&self, v: &VertexId) -> Vec<&VertexId> {
        let mut future = vec![];
        let mut toexplore = self.get_edges_to_vertex(v);
        let mut seen = vec![v];

        while toexplore.len() > 0 {
            let head = toexplore.remove(0);
            if !seen.contains(&&head) {
                future.push(head);
                for parent in self.get_edges_to_vertex(&head) {
                    if !seen.contains(&parent) {
                        toexplore.push(parent);
                        seen.push(parent);
                    }
                }
            }
        }
        future
    }

    pub fn processes_in_future(&self, v: &VertexId, n: u32) -> Vec<u32>{
        let mut processes = vec![];
        let mut toexplore = self.get_edges_to_vertex(v);
        let mut seen = vec![v];

        while toexplore.len() > 0 && processes.len() < n as usize {
            let head = toexplore.remove(0);
            if !seen.contains(&&head) {
                if !processes.contains(&head.process_id) {
                    processes.push(head.process_id);
                }

                for parent in self.get_edges_to_vertex(&head) {
                    if !seen.contains(&parent) {
                        toexplore.push(parent);
                        seen.push(parent);
                    }
                }
            }
        }
        processes
    }

    pub fn first_from_processes (&self, start: &VertexId, processes: &Vec<&u32>) -> &VertexId {
        let mut toexplore = self.get_edges_to_vertex(&start);
        let mut seen = vec![start];

        while toexplore.len() > 0 {
            let head = toexplore.remove(0);
            if processes.contains(&&head.process_id) {  // BFS until we find a vertex from one of the processes
                return head;
            }

            for child in self.get_edges_to_vertex(head) {
                if !seen.contains(&child) {
                    seen.push(child);
                    toexplore.push(child);
                }
            }
        }
        &self.get_root().id // return root if no vertex found
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root() {
        let dag = Dag::new(0);
        let root = dag.get_root();
        assert!(root.id == VertexId::new(0, 0));
    }
}