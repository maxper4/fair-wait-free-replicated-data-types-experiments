use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Sender, Receiver};

use crate::config::Config;
use crate::crdt::OperationParameter;
use crate::process::CRDTOperationMessage;

use bincode;

pub async fn run<P>(config: &Config) -> (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>, tokio::task::JoinHandle<()>) where P: OperationParameter + DeserializeOwned {
    let (local_to_peers_sender, local_to_peers_receiver): (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>) = tokio::sync::mpsc::channel(100);
    let (peers_to_local_sender, peers_to_local_receiver): (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>) = tokio::sync::mpsc::channel(100);
    let ip = config.ip.clone();
    let peers = config.peers.clone();

    let accept_con_taks = tokio::spawn(async move {
        accept_connections(ip, peers_to_local_sender).await;
    });

    let mut out_peers = vec![];
    for peer in peers {  // TODO dynamicly add/remove peers
        let stream = TcpStream::connect(peer.ip).await;    
        match stream {
            Ok(stream) => out_peers.push(stream),
            Err(_) => println!("Failed to connect to peer"),// TODO handle failure
        }
    }

    let talk_task = tokio::spawn(async move {
        talk(local_to_peers_receiver, out_peers).await;
    });

    let network_task = tokio::spawn(async move {
        tokio::join!(accept_con_taks, talk_task);
    });
    (local_to_peers_sender, peers_to_local_receiver, network_task)
}

async fn accept_connections<P>(ip: String, peers_to_local_sender: Sender<CRDTOperationMessage<P>>) where P: OperationParameter + DeserializeOwned {
    let listener = TcpListener::bind(ip).await.unwrap();
    loop {
        match listener.accept().await {
            Ok((stream, _)) => { 
                let peers_to_local_sender = peers_to_local_sender.clone();
                tokio::spawn(async move {
                    listen_peer(stream, peers_to_local_sender).await;
                });
            },
            Err(e) => println!("Accept connection failed: {:?}", e),
        }
    }
}

async fn listen_peer<P>(mut stream: TcpStream, peers_to_local_sender: Sender<CRDTOperationMessage<P>>) where P: OperationParameter + DeserializeOwned {
    loop {
        match stream.read_u64().await {
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!("Connection closed by peer {}", stream.peer_addr().unwrap());
                break;
            },
            Ok(0) => {
                println!("Connection closed by peer {}", stream.peer_addr().unwrap());
                break;
            },
            Ok(len) => {
                let mut buf = vec![0; len as usize];

                match stream.read(&mut buf).await {
                    Ok(_) => {
                        match bincode::deserialize(&buf) {
                            Ok(msg) => {
                                peers_to_local_sender.send(msg).await.unwrap(); // TODO handle failure
                            },
                            Err(e) => {
                                println!("Error deserializing message from peer {}: {}", stream.peer_addr().unwrap(), e);
                                continue;
                            }
                        }
                    },
                    Err(e) => {
                        println!("Error reading buffer {}: {}", stream.peer_addr().unwrap(), e);
                        break;
                    }
                }
            },
            Err(e) => {
                println!("Error listen peer {}: {}", stream.peer_addr().unwrap(), e);
                break;
            }
        }        
    }
}

async fn talk<P>(mut local_to_peers_receiver: Receiver<CRDTOperationMessage<P>>, mut out_peers: Vec<TcpStream>) where P: OperationParameter {
    while let Some(msg) = local_to_peers_receiver.recv().await {
        match bincode::serialize(&msg) {
            Ok(bytes) => {
                for peer in &mut out_peers {
                    match peer.write_u64(bytes.len() as u64).await {
                        Ok(_) => {
                            match peer.write(&bytes).await {
                                Ok(_) => {},
                                Err(e) => {
                                    println!("Error sending message to peer {}: {}", peer.peer_addr().unwrap(), e);
                                }
                            }
                        },
                        Err(e) => {
                            println!("Error sending message length to peer {}: {}", peer.peer_addr().unwrap(), e);
                        }
                    }
                }
            },
            Err(e) => {
                println!("Error serializing message: {}", e);
            },
        }
    }
}