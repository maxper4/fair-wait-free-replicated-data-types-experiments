use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Sender, Receiver};

use crate::config::Config;
use crate::crdt::OperationParameter;
use crate::process::CRDTOperationMessage;

use bincode;

pub async fn run<P>(config: Config) -> (Sender<CRDTOperationMessage<P>>, tokio::task::JoinHandle<()>) where P:OperationParameter + DeserializeOwned {
    let (local_to_peers_sender, local_to_peers_receiver): (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>) = tokio::sync::mpsc::channel(100);
    let (peers_to_local_sender, peers_to_local_receiver): (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>) = tokio::sync::mpsc::channel(100);

    let accept_con_taks = tokio::spawn(async move {
        accept_connections(config.ip, peers_to_local_sender).await;
    });

    let mut out_peers = vec![];
    for peer in config.peers {  // TODO dynamicly add/remove peers
        let stream = TcpStream::connect(peer.ip).await;    
        match stream {
            Ok(stream) => out_peers.push(stream),
            Err(_) => println!("Failed to connect to peer"),// TODO handle failure
        }
    }

    let listen_task = tokio::spawn(async move {
        listen(peers_to_local_receiver).await;
    });

    let talk_task = tokio::spawn(async move {
        talk(local_to_peers_receiver, out_peers).await;
    });

    let network_task = tokio::spawn(async move {
        tokio::join!(accept_con_taks, listen_task, talk_task);
    });
    (local_to_peers_sender, network_task)
}

async fn accept_connections<P>(ip: String, peers_to_local_sender: Sender<CRDTOperationMessage<P>>) where P: OperationParameter + DeserializeOwned {
    let listener = TcpListener::bind(ip).await.unwrap();
    loop {
        let (stream, _) = listener.accept().await.unwrap(); // TODO handle failure
        let peers_to_local_sender = peers_to_local_sender.clone();
        tokio::spawn(async move {
            listen_peer(stream, peers_to_local_sender).await;
        });
        
    }
}

async fn listen<P>(mut peers_to_local_receiver: Receiver<CRDTOperationMessage<P>>) where P: OperationParameter{
    while let Some(msg) = peers_to_local_receiver.recv().await {
        println!("Received: {}", msg.to_string());
    }
}

async fn listen_peer<P>(mut stream: TcpStream, peers_to_local_sender: Sender<CRDTOperationMessage<P>>) where P: OperationParameter + DeserializeOwned{
    loop {
        let len = stream.read_u64().await.unwrap(); // TODO handle failure
        let mut buf = vec![0; len as usize];
        stream.read(&mut buf).await.unwrap(); // TODO handle failure, closing

        let msg = bincode::deserialize(&buf).unwrap(); // TODO handle failure
        peers_to_local_sender.send(msg).await.unwrap(); // TODO handle failure
    }
}

async fn talk<P>(mut local_to_peers_receiver: Receiver<CRDTOperationMessage<P>>, mut out_peers: Vec<TcpStream>) where P: OperationParameter {
    while let Some(msg) = local_to_peers_receiver.recv().await {
        let bytes = bincode::serialize(&msg).unwrap(); // TODO handle failure

        for peer in &mut out_peers {
            peer.write_u64(bytes.len() as u64).await.unwrap(); // TODO handle failure
            peer.write(&bytes).await.unwrap(); // TODO handle failure
        }
    }
}