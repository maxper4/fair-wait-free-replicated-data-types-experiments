use std::future::Future;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;

use libp2p::futures::StreamExt;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use ::serde::de::DeserializeOwned;
use tokio::select;
use tokio::sync::mpsc::{Sender, Receiver};

use libp2p::{Multiaddr, gossipsub, noise, tcp, yamux};

use crate::config::{Config};
use crate::crdt::OperationParameter;
use crate::date;
use crate::process::CRDTOperationMessage;

#[derive(NetworkBehaviour)]
struct GossipNetworkBehaviour {
    gossipsub: gossipsub::Behaviour,
}

pub async fn run<P>(config: &Config) -> (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>, impl Future) where P: OperationParameter + DeserializeOwned {
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        ).unwrap().with_behaviour(|key| {
            // To content-address message, we can take the hash of message and use it as an ID. // if we hash we cannot send the same termination signal for each process because it is considered a duplicate
            // let message_id_fn = |message: &gossipsub::Message| {
            //     let mut s = DefaultHasher::new();
            //     message.data.hash(&mut s);
            //     gossipsub::MessageId::from(s.finish().to_string())
            // };

            // Set a custom gossipsub configuration
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(1)) // This is set to aid debugging by not cluttering the log space
                .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message
                // signing)
               // .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
                .build().unwrap();

            // build a gossipsub network behaviour
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            Ok(GossipNetworkBehaviour { gossipsub })
        }).unwrap()
        .with_swarm_config(|cfg| {
            cfg.with_idle_connection_timeout(Duration::from_secs(u64::MAX))
        })
        .with_connection_timeout(Duration::from_secs(5))
        .build();

    let topic = gossipsub::IdentTopic::new("crdt-broadcast");
    swarm.behaviour_mut().gossipsub.subscribe(&topic).unwrap();

    swarm.listen_on(format!("/ip4/0.0.0.0/tcp/{}", config.port.clone()).parse().unwrap()).unwrap();

    let (local_to_peers_sender, mut local_to_peers_receiver): (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>) = tokio::sync::mpsc::channel(100);
    let (peers_to_local_sender, peers_to_local_receiver): (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>) = tokio::sync::mpsc::channel(100);

    let bincodeconfig = bincode::config::standard().with_big_endian().with_no_limit();
    
    let topic_hash = topic.hash();

    let n = config.peers.len();
    let mut working_processes = n;

    for peer in config.peers.clone() {
        let remote: Multiaddr = format!("/ip4/{}/tcp/{}", peer.ip.clone(),peer.port.clone()).parse().unwrap();
        let dialops = DialOpts::unknown_peer_id().address(remote);
        swarm.dial(dialops.build()).unwrap();
    }

    while swarm.behaviour_mut().gossipsub.all_peers().filter(|(_, topics)| topics.contains(&&topic_hash)).count() < n {
        let peers: Vec<_> = swarm.behaviour_mut().gossipsub.all_peers().collect();
        println!("Waiting for peers to connect... {}/{}", peers.len(), n);

        match swarm.select_next_some().await  {
            SwarmEvent::Behaviour(GossipNetworkBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                propagation_source: peer_id,
                message_id: id,
                message,
            })) => {
                if message.data.is_empty() {
                    println!("Received shutdown message from peer {peer_id}");
                    working_processes -= 1;
                } else {
                    match bincode::serde::decode_from_slice::<CRDTOperationMessage<P>, bincode::config::Configuration<_>>(&message.data, bincodeconfig) {
                        Ok((msg, _)) => {
                            println!("Got message with id {id} from peer {peer_id}");
                            peers_to_local_sender.send(msg).await.unwrap();
                            break; // someone started, let's start too
                        },
                        Err(e) => {
                            println!("Error deserializing message from peer {} for msg: {:?} : {}", peer_id, message.data, e);
                            continue;
                        }
                    }
                }
            },
            SwarmEvent::OutgoingConnectionError { error, .. } => {
                match error {
                    libp2p::swarm::DialError::Transport(e) => { // timed out so we try dialing again
                        let (address, _) = &e[0];
                        swarm.dial(address.clone()).unwrap();
                    },
                    _ => {
                        println!("Other error when trying to connect to a peer: {error}");
                    },
                }
            },
            _ => {}
        }
    }

    let network_task = tokio::spawn(async move {
        loop {
        select! {
            local_to_peers = local_to_peers_receiver.recv() => {
                match local_to_peers {
                    Some(msg) => {
                        match bincode::serde::encode_to_vec(&msg, bincodeconfig) {
                            Ok(bytes) => {
                                if let Err(e) = swarm
                                .behaviour_mut().gossipsub
                                .publish(topic.clone(), bytes) {
                                    println!("Publish error: {e:?}");
                                }
                                println!("Sent message to peers: {}", msg.to_string());
                            },
                            Err(e) => {
                                println!("Error serializing message: {}", e);
                            },
                        }                       },
                    None => {
                        println!("Local to peers channel closed, sending shutdown.");
                        swarm.behaviour_mut().gossipsub
                                .publish(topic.clone(), vec![]).unwrap();
                        break;
                    }
                }
            },
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(GossipNetworkBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message_id: id,
                    message,
                })) => {
                    if message.data.is_empty() {
                        println!("Received shutdown message from peer {peer_id}");
                        working_processes -= 1;
                    } else {
                        match bincode::serde::decode_from_slice::<CRDTOperationMessage<P>, bincode::config::Configuration<_>>(&message.data, bincodeconfig) {
                            Ok((msg, _)) => {
                                println!("Got message with id {id} from peer {peer_id}:{}", msg.to_string());
                                peers_to_local_sender.send(msg).await.unwrap();
                            },
                            Err(e) => {
                                println!("Error deserializing message from peer {} for msg: {:?} : {}", peer_id, message.data, e);
                                continue;
                            }
                        }
                    }
                }
                SwarmEvent::OutgoingConnectionError { error, .. } => {
                    match error {
                        libp2p::swarm::DialError::Transport(e) => { // timed out so we try dialing again
                            let (address, _) = &e[0];
                            swarm.dial(address.clone()).unwrap();
                        },
                        _ => {
                            println!("Other error when trying to connect to a peer: {error}");
                        },
                    }
                },
                _ => {}
            },
            else => {
                break;
            }
        }
        }

        while working_processes > 0 {
        let event = swarm.next().await;
        match event {
            Some(SwarmEvent::Behaviour(GossipNetworkBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                propagation_source: peer_id,
                message_id: id,
                message,
            }))) => {
                if message.data.is_empty(){
                    println!("Received shutdown message from peer {peer_id}");
                    working_processes -= 1;
                } else {
                    match bincode::serde::decode_from_slice::<CRDTOperationMessage<P>, bincode::config::Configuration<_>>(&message.data, bincodeconfig) {
                        Ok((msg, _)) => {
                            println!("Got message with id {id} from peer {peer_id}:{}", msg.to_string());
                            peers_to_local_sender.send(msg).await.unwrap();
                        },
                        Err(e) => {
                            println!("Error deserializing message from peer {}: {}", peer_id, e);
                        }
                    }
                }
            },
            Some(SwarmEvent::OutgoingConnectionError { error, .. }) => {
                match error {
                    libp2p::swarm::DialError::Transport(e) => { // timed out so we try dialing again
                        let (address, _) = &e[0];
                        swarm.dial(address.clone()).unwrap();
                    },
                    _ => {
                        println!("Other error when trying to connect to a peer: {error}");
                    },
                }
            },
            _ => {}
            }
        }
        tokio::time::timeout(tokio::time::Duration::from_secs(60), async {
            loop {
                swarm.next().await;
            }
        }).await.unwrap_err();
    });

    (local_to_peers_sender, peers_to_local_receiver, network_task)
}