use zookeeper;
use std::sync::mpsc::{Sender, Receiver, channel, RecvTimeoutError};
use std::collections::{HashSet, HashMap};
use zookeeper::election::LeaderElector;
use zookeeper::message::{MessageType, Message, NodeState};
use std::thread;

#[test]
fn one_election() {
    n_election(vec![(0,0,0,0)], Some((0,0)));
}

fn n_election(params: Vec<(u64, u64, u64, u64)>, expected_result: Option<(u64, u64)>) {
    let n = params.len() as u64;
    let quorum_size = n / 2 + 1;

    let mut senders : HashMap<u64, Sender<Message>> = HashMap::new();
    let mut electors : HashMap<u64, LeaderElector> = HashMap::new();
    let mut receivers : HashMap<u64, Receiver<Message>> = HashMap::new();
    
    for (id, election_epoch, _, _) in params.clone() {
        electors.insert(id, LeaderElector::new(id, election_epoch, quorum_size));
        let (sender, receiver) = channel();
        senders.insert(id, sender.clone());
        receivers.insert(id, receiver);
    }

    let mut handles : HashMap<u64, thread::JoinHandle<Option<(u64, u64)>>> = HashMap::new();
    for (id, _, init_proposed_zab_epoch, last_zxid) in params.clone() {
        let mut electors_cpy = electors.remove(&id).unwrap();
        let mut receivers_cpy = receivers.remove(&id).unwrap();
        let mut senders_cpy = senders.clone();
        let handle = thread::spawn(move || {
            return electors_cpy.look_for_leader(&mut receivers_cpy, &mut senders_cpy, init_proposed_zab_epoch, last_zxid, quorum_size);
        });
        handles.insert(id, handle);
    }

    for (id,handle) in handles {
        let res = handle.join().unwrap();
        println!("{}", id);
        assert_eq!(res, expected_result);
    }
}