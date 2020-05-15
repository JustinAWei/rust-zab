use zookeeper;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::collections::{HashMap};
use zookeeper::election::LeaderElector;
use zookeeper::message::{Message};
use std::thread;

// vec id, epoch, zepoch, zxid
// result (zab_epoch, id)

// cmp zepoch
#[test]
fn three_zab_epoch_cmp_0() {
    n_election(vec![(0,0,2,0), (1,0,3,0), (2,0,1,0)], Some((3,1)));
}

#[test]
fn three_zab_epoch_cmp_1() {
    n_election(vec![(0,0,4,0), (1,0,3,0), (2,0,1,0)], Some((4,0)));
}

#[test]
fn three_zab_epoch_cmp_2() {
    n_election(vec![(0,0,2,0), (1,0,3,0), (2,0,5,0)], Some((5,2)));
}

#[test]
fn three_zab_epoch_cmp_3() {
    n_election(vec![(0,0,3,0), (1,0,3,0), (2,0,2,0)], Some((3,1)));
}

#[test]
fn three_zab_epoch_cmp_1a() {
    n_election(vec![(0,0,2,2), (1,0,3,50), (2,0,1,20)], Some((3,1)));
}

#[test]
fn three_zab_epoch_cmp_2a() {
    n_election(vec![(0,0,4,10), (1,0,3,120), (2,0,1,4)], Some((4,0)));
}

#[test]
fn three_zab_epoch_cmp_3a() {
    n_election(vec![(21340,0,2,1), (64321,0,3,2), (2,0,5,3)], Some((5,2)));
}

#[test]
fn three_zab_epoch_cmp_4a() {
    n_election(vec![(0,0,3,76), (12341,0,3,23), (7652,0,2,1111)], Some((3,0)));
}

#[test]
fn three_zab_epoch_cmp_1aa() {
    n_election(vec![(65340,0,2,2), (1,0,3,50), (21342,0,1,20)], Some((3,1)));
}

#[test]
fn three_zab_epoch_cmp_2aa() {
    n_election(vec![(0,0,4,10), (1674,0,3,120), (1232,0,1,4)], Some((4,0)));
}

#[test]
fn three_zab_epoch_cmp_3aa() {
    n_election(vec![(440,0,2,1), (991,0,3,2), (2,0,5,3)], Some((5,2)));
}

#[test]
fn three_zab_epoch_cmp_4aa() {
    n_election(vec![(0,0,3,76), (1,0,3,23), (2,0,2,1111)], Some((3,0)));
}

// cmp zxid
#[test]
fn three_zxid_cmp_0() {
    n_election(vec![(0,0,0,2), (1,0,0,0), (2,0,0,0)], Some((0,0)));
}

#[test]
fn three_zxid_cmp_1() {
    n_election(vec![(0,0,0,2), (1,0,0,3), (2,0,0,0)], Some((0,1)));
}

#[test]
fn three_zxid_cmp_2() {
    n_election(vec![(0,0,0,2), (1,0,0,1), (2,0,0,4)], Some((0,2)));
}

#[test]
fn three_zxid_cmp_3() {
    n_election(vec![(0,0,0,1), (1,0,0,2), (2,0,0,1)], Some((0,1)));
}

// cmp zxid
#[test]
fn three_zxid_cmp_0a() {
    n_election(vec![(0,0,0,2), (131244,0,0,0), (12342,0,0,0)], Some((0,0)));
}

#[test]
fn three_zxid_cmp_1a() {
    n_election(vec![(650,0,0,2), (1,0,0,3), (12342,0,0,0)], Some((0,1)));
}

#[test]
fn three_zxid_cmp_2a() {
    n_election(vec![(32140,0,0,2), (23421,0,0,1), (2,0,0,4)], Some((0,2)));
}

#[test]
fn three_zxid_cmp_3a() {
    n_election(vec![(12340,0,0,1), (1,0,0,2), (5682,0,0,1)], Some((0,1)));
}

// cmp on id
#[test]
fn three_election_id_cmp_1() {
    n_election(vec![(0,0,0,0), (1,0,0,0), (2,0,0,0)], Some((0,2)));
}

#[test]
fn five_election_id_cmp_1() {
    n_election(vec![(0,0,0,0), (1,0,0,0), (2,0,0,0), (3,0,0,0), (4,0,0,0)], Some((0,4)));
}

#[test]
fn three_election_id_cmp_1a() {
    n_election(vec![(0,1,1,1), (1,1,1,1), (2,1,1,1)], Some((1,2)));
}

#[test]
fn five_election_id_cmp_1a() {
    n_election(vec![(0,1,1,1), (1,1,1,1), (2,1,1,1), (3,1,1,1), (4,1,1,1)], Some((1,4)));
}

// cmp on election epoch
#[test]
fn three_election_epoch_cmp_1() {
    n_election(vec![(0,1,0,0), (1,1,0,0), (2,1,0,0)], Some((0,2)));
}

#[test]
fn three_election_epoch_cmp_2() {
    n_election(vec![(0,0,0,0), (1,0,0,0), (2,1,0,0)], Some((0,2)));
}

#[test]
fn three_election_epoch_cmp_3() {
    n_election(vec![(0,0,0,0), (1,0,0,0), (2,2,0,0)], Some((0,2)));
}

#[test]
fn three_election_epoch_cmp_4() {
    n_election(vec![(0,0,0,0), (1,0,0,0), (2,20,0,0)], Some((0,2)));
}

#[test]
fn three_election_epoch_cmp_5() {
    n_election(vec![(0,0,0,0), (1,0,0,0), (2,100,0,0)], Some((0,2)));
}

// cmp on election epoch
#[test]
fn three_election_epoch_cmp_1a() {
    n_election(vec![(0,1,0,0), (1,1,0,0), (2,10,0,0)], Some((0,2)));
}

#[test]
fn three_election_epoch_cmp_2a() {
    n_election(vec![(0,5,0,0), (1,10,0,0), (2,20,0,0)], Some((0,2)));
}

#[test]
fn three_election_epoch_cmp_3a() {
    n_election(vec![(0,1,0,0), (1,2,0,0), (2,3,0,0)], Some((0,2)));
}

#[test]
fn fifty_one_election() {
    let mut params : Vec<(u64, u64, u64, u64)> = Vec::new();
    for i in 0..51 {
        params.push((i,0,0,0));
    }
    n_election(params, Some((0,50)));
}

fn n_election(params: Vec<(u64, u64, u64, u64)>, expected_result: Option<(u64, u64)>) {
    let n = params.len() as u64;
    let quorum_size = n;
    // let quorum_size = n / 2 + 1;

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
            let mut r = None;
            senders_cpy.remove(&id).unwrap();
            while r.is_none() {
                r = electors_cpy.look_for_leader(&mut receivers_cpy, &mut senders_cpy, init_proposed_zab_epoch, last_zxid);
            }
            return r;
        });
        handles.insert(id, handle);
    }

    for (id,handle) in handles {
        let res = handle.join().unwrap();
        // println!("{}", id);
        assert_eq!(res, expected_result);
    }
}