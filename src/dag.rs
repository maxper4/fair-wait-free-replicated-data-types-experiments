use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
pub struct Vertex<T> {
    pub id: u32,
    pub label: T,
}

impl <T>Vertex<T> {
    pub fn new(id: u32, l: T) -> Vertex<T> {
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
    edges: HashMap<u32, Vec<u32>>, // from -> to
}

impl<T> Dag<T> {
    pub fn new(init: T) -> Dag< T> {
        Dag {
            vertices: vec![Vertex::new(0, init)],
            edges: HashMap::new(),
        }
    }

    pub fn add_vertex(&mut self, parents: Vec<u32>, label: T) {
        self.vertices.push(Vertex::new(self.vertices.len() as u32, label));
        let v = &self.vertices[self.vertices.len() - 1];
        let parents_len = parents.len();
        for v2 in parents {
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
                    p.push(0);
                },
                None => {
                    self.edges.insert(v.id, vec![0]);
                }
                
            }
        }
    }

    pub fn get_root(&self) -> &Vertex<T> {
        &self.vertices[0]
    }

    pub fn get_edges_to_vertex(&self, id: usize) -> Vec<&Vertex<T>> {
        let mut edges = vec![];
        for v in &self.vertices {
            if let Some(parents) = self.edges.get(&(v.id)) {
                if parents.contains(&(id as u32)) {
                    edges.push(v);
                }
            }
        }

        edges
    }

    pub fn get_heads(&self) -> Vec<u32> {
        let mut heads = vec![];
        for v in &self.vertices {
             if self.get_edges_to_vertex(v.id as usize).len() == 0 {
                heads.push(v.id);
             }
        }

        heads
    }
}
