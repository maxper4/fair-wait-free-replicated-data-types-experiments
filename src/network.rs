use std::future::Future;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;

use libp2p::futures::StreamExt;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use ::serde::de::DeserializeOwned;
use tokio::select;
use tokio::sync::mpsc::{Sender, Receiver};

use libp2p::{Multiaddr, gossipsub, mdns, noise, tcp, yamux};

use crate::config::{Config};
use crate::crdt::OperationParameter;
use crate::process::CRDTOperationMessage;
use crate::date;

use bincode::{config, serde};

#[derive(NetworkBehaviour)]
struct GossipNetworkBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

pub async fn run<P>(config: &Config) -> (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>, impl Future) where P: OperationParameter + DeserializeOwned {
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        ).unwrap().with_behaviour(|key| {
            // To content-address message, we can take the hash of message and use it as an ID.
            let message_id_fn = |message: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_string())
            };

            // Set a custom gossipsub configuration
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message
                // signing)
                .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
                .build().unwrap();

            // build a gossipsub network behaviour
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;
            Ok(GossipNetworkBehaviour { gossipsub, mdns })
        }).unwrap()
        .with_swarm_config(|cfg| {
            cfg.with_idle_connection_timeout(Duration::from_secs(u64::MAX))
        }).build();

    let topic = gossipsub::IdentTopic::new("crdt-broadcast");
    swarm.behaviour_mut().gossipsub.subscribe(&topic).unwrap();

    //swarm.listen_on(("/ip4/"+config.ip.clone()+"/tcp/"+config.port.clone()).parse().unwrap()).unwrap();
    swarm.listen_on(format!("/ip4/0.0.0.0/tcp/{}", config.port.clone()).parse().unwrap()).unwrap();

    
    for peer in config.peers.clone() {
        let remote: Multiaddr = (peer.ip.clone() + "/tcp/" + &peer.port.clone()).parse().unwrap();
        swarm.dial(remote).unwrap();
    }

    let (local_to_peers_sender, mut local_to_peers_receiver): (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>) = tokio::sync::mpsc::channel(100);
    let (peers_to_local_sender, peers_to_local_receiver): (Sender<CRDTOperationMessage<P>>, Receiver<CRDTOperationMessage<P>>) = tokio::sync::mpsc::channel(100);

    let bincodeconfig = bincode::config::standard().with_big_endian().with_no_limit();

    let network_task = tokio::spawn(async move {
        loop {
        select! {
            Some(msg) = local_to_peers_receiver.recv() => {
                match bincode::serde::encode_to_vec(&msg, bincodeconfig) {
                    Ok(bytes) => {
                        if let Err(e) = swarm
                        .behaviour_mut().gossipsub
                        .publish(topic.clone(), bytes) {
                            println!("Publish error: {e:?}");
                        }
                    },
                    Err(e) => {
                        println!("Error serializing message: {}", e);
                    },
                }
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(GossipNetworkBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered a new peer: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    }
                },
                SwarmEvent::Behaviour(GossipNetworkBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discover peer has expired: {peer_id}");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    }
                },
                SwarmEvent::Behaviour(GossipNetworkBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message_id: id,
                    message,
                })) => {
                    match bincode::serde::decode_from_slice::<CRDTOperationMessage<P>, config::Configuration<_>>(&message.data, bincodeconfig) {
                        Ok((msg, _)) => {
                            println!("Got message with id {id} from peer {peer_id}:{}", msg.to_string());
                            peers_to_local_sender.send(msg).await.unwrap();
                        },
                        Err(e) => {
                            println!("Error deserializing message from peer {}: {}", peer_id, e);
                            continue;
                        }
                    }
                },
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Local node is listening on {address}");
                }
                _ => {}
            }
        }
    }
    });

    (local_to_peers_sender, peers_to_local_receiver, network_task)
}