use std::{fs, io::Read};

use serde::{Deserialize};

#[derive(Deserialize, Clone)]
pub struct Config {
   pub id: u32,
   pub ip: String,
   pub peers: Vec<Peer>,
}

#[derive(Deserialize, Clone)]
pub struct Peer {
    pub ip: String,
}

impl Config {
    pub fn get(file_name: &str) -> Config {
        // Read the file and return the Config object
        let mut file = fs::OpenOptions::new();
        file.read(true);
        let mut file = file.open(format!("{file_name}")).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        let config: Config = toml::from_str(&contents).unwrap();
        config
    }
}