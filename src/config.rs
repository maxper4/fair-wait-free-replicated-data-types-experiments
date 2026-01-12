use std::{fs, io::Read};

use serde::{Deserialize};

#[derive(Deserialize, Clone)]
pub struct Config {
<<<<<<< HEAD
    pub id: u32,
    pub ip: String,
    pub port: String,
    pub peers: Vec<Peer>,
    pub reconciliation_function: u8,
    pub duration: u64,
=======
   pub id: u32,
   pub ip: String,
   pub peers: Vec<Peer>,
   pub reconciliation_function: u8,
   pub data_type: u8,
   pub duration: u64,
>>>>>>> Data type of remove wins set
}

#[derive(Deserialize, Clone)]
pub struct Peer {
    pub ip: String,
    pub port: String,
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