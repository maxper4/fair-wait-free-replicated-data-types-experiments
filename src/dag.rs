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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}", self.process_id, self.local_id)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Vertex<T> where T: Clone {
    pub id: VertexId,
    pub label: T,
    pub distance: u32
}

impl <T>Vertex<T> where T: Clone {
    pub fn new(id: VertexId, l: T) -> Vertex<T> {
        Vertex { id, label: l, distance: 0 }
    }
}

impl <T>fmt::Display for Vertex<T> where T: Clone {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Vertex: (id: {}, distance: {})", self.id, self.distance)
    }
}

#[derive(Debug, Clone)]
pub struct Dag<T> where T: Clone {
    vertices: Vec<Vertex<T>>,
    edges: HashMap<VertexId, Vec<VertexId>>, // from -> to (to is the causal past of from)
    pub length: u32
}

impl<T> Dag<T> where T: Clone {
    pub fn new(init: T) -> Dag<T> {
        Dag {
            vertices: vec![Vertex::new(VertexId::new(0, 0), init)],
            edges: HashMap::new(),
            length: 1
        }
    }

    pub fn add_vertex(&mut self, parents: Vec<VertexId>, mut v: Vertex<T>) -> bool {
        for p in &parents {
            if !self.contains_vertex(p) {
                return false;
            }
        }

        if v.distance == 0 {
            v.distance = parents.iter().map(|p| self.get_vertex(p).unwrap_or_else(|| self.get_root()).distance).max().unwrap_or(0) + 1;
        }

        if self.length < v.distance {
            self.length = v.distance;
        }

        self.vertices.push(v);
        let v = &self.vertices[self.vertices.len() - 1];
        let parents_len = parents.len();
        let current_parents = self.edges.get_mut(&v.id);

        if parents_len == 0 {
            match current_parents {
                Some(p) => {
                    if p.len() == 0 {
                        p.push(VertexId::new(0, 0));
                    }
                },
                None => {
                    self.edges.insert(v.id, vec![VertexId::new(0, 0)]);
                }
                
            }
        }
        else {
            match current_parents {
                Some(p) => {
                    for v2 in &parents {
                        if !p.contains(v2) {
                            p.push(*v2);
                        }
                    }
                },
                None => {
                    self.edges.insert(v.id, parents.clone());
                }
            }
        }

        true
    }

    pub fn get_root(&self) -> &Vertex<T> {
        &self.vertices[0]
    }

    pub fn contains_vertex(&self, id: &VertexId) -> bool {
        for v in &self.vertices {
            if v.id == *id {
                return true;
            }
        }

        false
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
        let root = self.get_root().id;
        heads.retain(|x| *x != root); // remove root from heads
        heads
    }

    pub fn get_all_ids(&self) -> Vec<&VertexId> {
        self.vertices.iter().map(|v| &v.id).collect()
    }

    pub fn sorted_past(&self, start: Vec<&VertexId>, explored: &HashMap<VertexId, bool>) -> Vec<VertexId> {
        let mut toexplore = vec![];
        let mut past = vec![];

        for v in start.clone() {
            toexplore.push(*v);
        }
        let mut seen = explored.clone();

        while let Some(head) = toexplore.pop() {    // DFS
            if !seen.get(&head).unwrap_or(&false) {
                seen.insert(head, true);
                past.push(head.clone());

                let mut parents = self.get_edges_from_vertex(&head);
                parents.sort_by(|x, y| (*x).cmp(y));

                for parent in parents {
                    if !seen.get(&parent).unwrap_or(&false) {
                        toexplore.push(parent);
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
        toexplore.sort_by(|x, y| (**x).cmp(*y));
        let mut seen = vec![v];

        while toexplore.len() > 0 {
            let head = toexplore.remove(0);
            if !seen.contains(&&head) {
                seen.push(&head);
                future.push(head);

                let mut parents = self.get_edges_to_vertex(&head);
                parents.sort_by(|x, y| (**x).cmp(*y));

                for parent in parents {
                    if !seen.contains(&parent) {
                        toexplore.push(parent);
                    }
                }
            }
        }
        future
    }

    pub fn processes_in_future(&self, v: &VertexId, n: u32) -> Vec<u32>{
        let mut processes = vec![];
        let mut toexplore = self.get_edges_to_vertex(v);
        toexplore.sort_by(|x, y| (**x).cmp(*y));
        let mut seen = vec![v];

        while toexplore.len() > 0 && processes.len() < n as usize {
            let head = toexplore.remove(0);
            if !seen.contains(&&head) {
                seen.push(&head);
                if !processes.contains(&head.process_id) {
                    processes.push(head.process_id);
                }

                let mut parents = self.get_edges_to_vertex(&head);
                parents.sort_by(|x, y| (**x).cmp(*y));

                for parent in parents {
                    if !seen.contains(&parent) {
                        toexplore.push(parent);
                    }
                }
            }
        }
        processes
    }

    pub fn first_from_processes (&self, start: &VertexId, processes: &Vec<&u32>) -> &VertexId {
        let mut toexplore = self.get_edges_to_vertex(&start);
        toexplore.sort_by(|x, y| (**x).cmp(*y));

        let mut seen = vec![start];

        while toexplore.len() > 0 {
            let head = toexplore.remove(0);
            seen.push(&head);

            if processes.contains(&&head.process_id) {  // BFS until we find a vertex from one of the processes
                return head;
            }

            let mut children = self.get_edges_to_vertex(head);
            children.sort_by(|x, y| (**x).cmp(*y));

            for child in children {
                if !seen.contains(&child) {
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