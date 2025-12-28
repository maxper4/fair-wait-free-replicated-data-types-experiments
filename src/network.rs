use std::future::Future;

use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::config::{Config, Peer};
use crate::crdt::OperationParameter;
use crate::process::CRDTOperationMessage;
use crate::date;

use bincode;

pub async fn run<P>(config: &Config) -> (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>, impl Future) where P: OperationParameter + DeserializeOwned {
    let (local_to_peers_sender, local_to_peers_receiver): (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>) = tokio::sync::mpsc::channel(100);
    let (peers_to_local_sender, peers_to_local_receiver): (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>) = tokio::sync::mpsc::channel(100);
    let ip = config.ip.clone();
    let peers = config.peers.clone();

    let (in_peers, out_peers) = tokio::join!(accept_connections(ip, peers.len()), connect_to_peers(peers));

    let mut network_task = JoinSet::new();
    network_task.spawn(talk(local_to_peers_receiver, out_peers));

    for s in in_peers {
        network_task.spawn(listen_peer(s, peers_to_local_sender.clone()));
    }

    (local_to_peers_sender, peers_to_local_receiver, network_task.join_all())
}

async fn accept_connections(ip: String, nb: usize) -> Vec<TcpStream> {
    let listener = TcpListener::bind(ip).await.unwrap();
    let mut streams = vec![];

    for _ in 0..nb {
        loop {
            match listener.accept().await {
                    Ok((stream, _)) => { 
                        stream.set_linger(None).unwrap();
                        stream.set_nodelay(true).unwrap();

                        streams.push(stream);
                        break;
                    },
                    Err(e) => println!("Accept connection failed: {:?}", e),
            }
        }
    }

    return streams;
}

async fn connect_to_peers(peers: Vec<Peer>) -> Vec<TcpStream> {
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await; // wait a bit to let peers start listening

    let mut handles = JoinSet::new();

    for peer in peers {
        handles.spawn(async move {
            let mut stream = TcpStream::connect(peer.ip.clone()).await;
            loop {
                match stream {
                    Ok(stream) => { 
                        stream.set_linger(None).unwrap();
                        stream.set_nodelay(true).unwrap();

                        return stream; },
                    Err(e) => { 
                        println!("Error connecting to peer {}: {}. Retrying...", peer.ip, e);
                        stream = TcpStream::connect(peer.ip.clone()).await; 
                    },
                }
            }
        });
    }

    return handles.join_all().await;
}

async fn listen_peer<P>(mut stream: TcpStream, peers_to_local_sender: Sender<CRDTOperationMessage<P>>) where P: OperationParameter + DeserializeOwned {
    let addr = stream.peer_addr().unwrap();

    loop {
        println!("Listening to peer {} at {}", addr, date());
        
        match stream.read_u64().await { 
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!("Connection closed by peer {} at {}", addr, date());
                break;
            },
            Ok(0) => {
                println!("Received 0: Connection closed by peer {} at {}", addr, date());
                break;
            },
            Ok(len) => {
                let mut buf = vec![0; len as usize];
                println!("Expecting to read {} bytes from peer {}", len, addr);

                match stream.read(&mut buf).await {
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        println!("Connection closed by peer {}", addr);
                        break;
                    },
                    Ok(0) => {
                        println!("Received 0: Connection closed by peer {} at {}", addr, date());
                        break;
                    },
                    Ok(_) => match bincode::deserialize(&buf) {
                        Ok(msg) => {
                            println!("Read message from peer {}", addr);
                            peers_to_local_sender.send(msg).await.unwrap();
                        },
                        Err(e) => {
                            println!("Error deserializing message from peer {}: {}", addr, e);
                            continue;
                        }
                    },
                    Err(e) => {
                        println!("Error reading buffer {}: {}", addr, e);
                        break;
                    }
                }
            },
            Err(e) => {
                println!("Error listen peer {}: {}", addr, e);
                break;
            }
        }        
    }

    stream.write_u64(0).await.unwrap(); // ack termination
    println!("Listen task ended for peer {} at {}.", addr, date());
}

async fn talk<P>(mut local_to_peers_receiver: Receiver<CRDTOperationMessage<P>>, mut out_peers: Vec<TcpStream>) where P: OperationParameter {
    while let Some(msg) = local_to_peers_receiver.recv().await {
        match bincode::serialize(&msg) {
            Ok(bytes) => {
                for peer in &mut out_peers {

                   match peer.write_u64(bytes.len() as u64).await {
                       Ok(_) => {
                            match peer.write_all(&bytes).await {
                                Ok(()) => {
                                },
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

    println!("Trying sending shutdown msgs at {}.", date());

    let mut handles = JoinSet::new();

    for mut peer in out_peers {    // custom shutdown
        handles.spawn(async move {
            peer.write_u64(0).await.unwrap(); // signal termination
            let _ = timeout(tokio::time::Duration::from_secs(60), peer.read_u64()).await; // wait for ack
        });
    }

    handles.join_all().await;
}