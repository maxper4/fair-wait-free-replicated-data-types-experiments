use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt, join};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Sender, Receiver};

use crate::config::{Config, Peer};
use crate::crdt::OperationParameter;
use crate::process::CRDTOperationMessage;

use bincode;

pub async fn run<P>(config: &Config) -> (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>, tokio::task::JoinHandle<()>) where P: OperationParameter + DeserializeOwned {
    let (local_to_peers_sender, local_to_peers_receiver): (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>) = tokio::sync::mpsc::channel(100);
    let (peers_to_local_sender, peers_to_local_receiver): (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>) = tokio::sync::mpsc::channel(100);
    let ip = config.ip.clone();
    let peers = config.peers.clone();

    let (listening_tasks, out_peers) = tokio::join!(accept_connections(ip, peers_to_local_sender, peers.len()), connect_to_peers(peers));

    let network_task = tokio::spawn( async { 
        tokio::join!(
            async {
                for task in listening_tasks {
                    task.await.unwrap();
                }
            },
            talk(local_to_peers_receiver, out_peers),
    );});

    (local_to_peers_sender, peers_to_local_receiver, network_task)
}

async fn accept_connections<P>(ip: String, peers_to_local_sender: Sender<CRDTOperationMessage<P>>, nb: usize) -> Vec<tokio::task::JoinHandle<()>> where P: OperationParameter + DeserializeOwned {
    let listener = TcpListener::bind(ip).await.unwrap();
    let mut listening_tasks = vec![];

    for _ in 0..nb {
        loop {
            match listener.accept().await {
                    Ok((stream, _)) => { 
                        let peers_to_local_sender = peers_to_local_sender.clone();
                        listening_tasks.push(tokio::spawn(async move {
                            listen_peer(stream, peers_to_local_sender).await;
                        }));
                        break;
                    },
                    Err(e) => println!("Accept connection failed: {:?}", e),
            }
        }
    }

    return listening_tasks;
}

async fn connect_to_peers(peers: Vec<Peer>) -> Vec<TcpStream> {
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await; // wait a bit to let peers start listening

    let mut handles = vec![];

    for peer in peers {
        handles.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(peer.ip.clone()).await;
            loop {
                match stream {
                    Ok(stream) => { return stream; },
                    Err(e) => { 
                        println!("Error connecting to peer {}: {}. Retrying...", peer.ip, e);
                        stream = TcpStream::connect(peer.ip.clone()).await; 
                    },
                }
            }
        }));
    }

    let mut out_peers = vec![];

    for handle in handles {     // "join all"
        let stream = handle.await.unwrap();
        out_peers.push(stream);
    }

    return out_peers;
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