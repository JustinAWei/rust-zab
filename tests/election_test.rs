use zookeeper;
use std::sync::mpsc::{Sender, Receiver, channel, RecvTimeoutError};
use std::collections::{HashSet, HashMap};
use zookeeper::election::LeaderElector;
use zookeeper::message::{MessageType, Message, NodeState};

#[test]
fn single_node_election() {
    // one node
    let mut elector = LeaderElector::new(0, 0, 1);
    let (mut sender, mut receiver) = channel();
    let mut sender_map : HashMap<u64, Sender<Message>> = HashMap::new();
    sender_map.insert(0, sender);
    let r = elector.look_for_leader(&mut receiver, &mut sender_map, 0, 0, 1);
    assert_eq!(r, Some((0, 0)));
}