use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Sender, Receiver};

use crate::config::Config;

pub async fn run(config: Config) -> (Sender<String>, tokio::task::JoinHandle<()>) {
    let (local_to_peers_sender, local_to_peers_receiver): (Sender<String>, Receiver<String>) = tokio::sync::mpsc::channel(100);
    let (peers_to_local_sender, peers_to_local_receiver): (Sender<String>, Receiver<String>) = tokio::sync::mpsc::channel(100);

    let mut out_peers = vec![];
    for peer in config.peers {  // TODO dynamicly add/remove peers
        let stream = TcpStream::connect(peer.ip).await;    
        match stream {
            Ok(stream) => out_peers.push(stream),
            Err(_) => println!("Failed to connect to peer"),// TODO handle failure
        }
    }

    let accept_con_taks = tokio::spawn(async move {
        accept_connections(config.ip, peers_to_local_sender).await;
    });

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

async fn accept_connections(ip: String, peers_to_local_sender: Sender<String>) {
    let listener = TcpListener::bind(ip).await.unwrap();
    loop {
        let (stream, _) = listener.accept().await.unwrap(); // TODO handle failure
        let peers_to_local_sender = peers_to_local_sender.clone();
        tokio::spawn(async move {
            listen_peer(stream, peers_to_local_sender).await;
        });
        
    }
}

async fn listen(mut peers_to_local_receiver: Receiver<String>) {
    while let Some(msg) = peers_to_local_receiver.recv().await {
        println!("Received: {}", msg);
    }
}

async fn listen_peer(mut stream: TcpStream, peers_to_local_sender: Sender<String>) {
    loop {
        // let mut buf = [0; 1024];
        // let n = stream.read(&mut buf).await.unwrap(); // TODO handle failure
        // if n == 0 {
        //     break;
        // }
        // let msg = String::from_utf8_lossy(&buf[..n]).to_string();
        // peers_to_local_sender.send(msg).await.unwrap(); // TODO handle failure
    }
}

async fn talk(mut local_to_peers_receiver: Receiver<String>, out_peers: Vec<TcpStream>) {
    while let Some(msg) = local_to_peers_receiver.recv().await {
        for peer in &out_peers {
            //peer.write_all(msg.as_bytes()).await.unwrap(); // TODO handle failure
        }
    }
}