use rand::thread_rng;
use rand::seq::SliceRandom;
use std::{thread, time};
use zookeeper::zab_node::{Node, create_zab_ensemble};
use zookeeper::message::{MessageType, Message, NodeState};
use zookeeper::comm::{UnreliableSender, SenderController};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::fs::{create_dir, File};
use std::io::prelude::*;
use std::io::BufReader;
use std::io;

const results_filename   : &str = "logs/results.log";

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines(filename: &str) -> io::Result<io::Lines<io::BufReader<File>>> {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

// returns sender, controller, handles, running, arc to c. leader, arc to c. epoch
fn start_up_nodes(nnodes : u64) 
    -> (HashMap<u64, mpsc::Sender<Message>>,
        SenderController,
        HashMap<u64, JoinHandle<Node<UnreliableSender<Message>>>>,
        HashMap<u64, Arc<AtomicBool>>,
        Arc<AtomicU64>,
        Arc<AtomicU64>)
    {
    let (nodes, _senders, _controller) = create_zab_ensemble(nnodes);
    let mut handles : HashMap<u64, JoinHandle<Node<UnreliableSender<Message>>>> = HashMap::new();
    let curr_leader : Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let curr_epoch : Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let mut running : HashMap<u64, Arc<AtomicBool>> = HashMap::new();

    // start them up
    for (_node_id, mut node) in nodes {
        let curr_l = curr_leader.clone();
        let curr_e = curr_epoch.clone();
        let r = Arc::new(AtomicBool::new(true));
        running.insert(_node_id, r.clone());
        let handle = thread::spawn(move || {
            while r.load(Ordering::SeqCst) {
                node.main_loop();
                if node.epoch > curr_e.load(Ordering::SeqCst) && Some(node.id) == node.leader {
                    curr_l.store(node.id, Ordering::SeqCst);
                    curr_e.store(node.epoch, Ordering::SeqCst);
                }
            }
            return node;
        });
        handles.insert(_node_id, handle);
    }
    return (_senders, _controller, handles, running, curr_leader, curr_epoch)
}

fn kill(node_id : u64,
        senders : & HashMap<u64, mpsc::Sender<Message>>,
        handles : & mut HashMap<u64, JoinHandle<Node<UnreliableSender<Message>>>>,
        running : & mut HashMap<u64, Arc<AtomicBool>>) -> Node<UnreliableSender<Message>>
{
    running.get_mut(&node_id).unwrap().store(false, Ordering::SeqCst);
    let no_op = Message {
                    sender_id: 0,
                    epoch: 0,
                    msg_type: MessageType::ClientQuery,
                };
    senders.get(&node_id).unwrap().send(no_op.clone()).unwrap();
    let result = handles.remove(&node_id).unwrap().join().unwrap();
    return result;
}

fn get_truth() -> Vec<(u64, String)> {
    // Open the file in read-only mode with buffer.
    let mut results_history : Vec<(u64, String)>  = Vec::new();
    if let Ok(lines) = read_lines("./logs/results.log") {
        // Consumes the iterator, returns an (Optional) String
        for line in lines {
            if let Ok(ip) = line {
                let mut val : (u64, String) = serde_json::from_str(&ip).unwrap();
                results_history.push(val);
            }
        }
    }
    return results_history;
}

fn check_history_same(node_id : u64, truth: &Vec<(u64, String)>) -> bool {
    let mut node_history : Vec<(u64, String)>  = Vec::new();
    if let Ok(lines) = read_lines(&format!("./logs/{}.log", node_id)) {
        // Consumes the iterator, returns an (Optional) String
        for line in lines {
            if let Ok(ip) = line {
                let mut val : (String, u64, String) = serde_json::from_str(&ip).unwrap();
                if val.0 == "c" {
                    node_history.push((val.1, val.2));
                }
            }
        }
    }

    if node_history.len() != truth.len() {
        println!("wrong len! hist: {:?}, truth: {:?}", node_history, truth);
        return false;
    }
    for i in 0..node_history.len() {
        if node_history[i] != truth[i] {
            println!("wrong val at idx {}!", i);
            return false;
        }
    }
    return true;
}

fn check_history_prefix(node_id : u64, truth: &Vec<(u64, String)>) -> bool {
    let mut node_history : Vec<(u64, String)>  = Vec::new();
    if let Ok(lines) = read_lines(&format!("./logs/{}.log", node_id)) {
        // Consumes the iterator, returns an (Optional) String
        for line in lines {
            if let Ok(ip) = line {
                let mut val : (String, u64, String) = serde_json::from_str(&ip).unwrap();
                if val.0 == "c" {
                    node_history.push((val.1, val.2));
                }
            }
        }
    }

    if node_history.len() > truth.len() {
        return false;
    }
    for i in 0..node_history.len() {
        if node_history[i] != truth[i] {
            return false;
        }
    }
    return true;
}


#[test]
fn sanity_check_nodes() {
    let n = 5 as usize;
    let (senders, controller, mut handles, mut running, cl, ce) = start_up_nodes(n as u64);
    assert!(senders.len() == n);
    assert!(handles.len() == n);
    assert!(running.len() == n);

    
    let t = time::Duration::from_millis(400);
    thread::sleep(t);

    for i in 0..n {
        kill(i as u64, & senders, & mut handles, & mut running);
        assert!(handles.len() == n - i - 1);
    
    }
    let truth : Vec<(u64, String)> = Vec::new();

    for i in 0..n {
        assert!(check_history_same(i as u64, &truth))
    }
}

#[test]
fn network_partition_followers() {
    // Setup
    let n = 5 as usize;
    let (senders, controller, mut handles, mut running, cl, ce) = start_up_nodes(n as u64);
    assert!(senders.len() == n);
    assert!(handles.len() == n);
    assert!(running.len() == n);

    let t = time::Duration::from_millis(5000);
    thread::sleep(t);

    // Initial proposal
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("suhh")),
    };
    senders[&0].send(proposal.clone()).expect("nahh");
    let t = time::Duration::from_millis(500);
    thread::sleep(t);

    // Check invariants
    let truth = get_truth();
    for i in 0..n {
        assert!(check_history_same(i as u64, &truth), "failed at i = {}", i);
    }

    // Create partition with only followers
    let p_size = 2;
    let non_ldr: Vec<usize> = (0..n).filter(|id| *id as u64 != cl.load(Ordering::SeqCst)).collect();
    let p1: Vec<_> = non_ldr.choose_multiple(&mut rand::thread_rng(), p_size).collect();
    println!("partition {:?}", p1);
    for i in 0..n {
        if p1.contains(&&i) {
            for j in 0..n {
                if !p1.contains(&&j) {
                    controller.make_sender_fail(i as u64, j as u64);
                    controller.make_sender_fail(j as u64, i as u64);
                }
            }
        }
    }

    // Propose to partition
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("suhh1")),
    };
    let p_follower = *p1[0] as u64;
    senders[&p_follower].send(proposal.clone()).expect("nahh");
    let t = time::Duration::from_millis(500);
    thread::sleep(t);

    // Check invariants (no new commits)
    let truth = get_truth();
    for i in 0..n {
        assert!(check_history_same(i as u64, &truth), "failed at i = {}", i);
    }

    // Propose to quorum
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("suhh1")),
    };
    senders[&cl.load(Ordering::SeqCst)].send(proposal.clone()).expect("nahh");
    let t = time::Duration::from_millis(500);
    thread::sleep(t);

    // Check invariants (quorum should have committed, partition should not have)
    let mut same_count = 0;
    let truth = get_truth();
    for i in 0..n {
        assert!(check_history_prefix(i as u64, &truth), "failed at i = {}", i);
        if check_history_same(i as u64, &truth) {
            same_count += 1;
        }
    }
    assert!(same_count == n - p_size);

    // Restore partition and send proposal
    for i in 0..n {
        for j in 0..n {
            controller.make_sender_ok(i as u64, j as u64);
            controller.make_sender_ok(j as u64, i as u64);
        }
    }
    let t = time::Duration::from_millis(500);
    thread::sleep(t);
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("suhh2")),
    };
    senders[&cl.load(Ordering::SeqCst)].send(proposal.clone()).expect("nahh");
    let t = time::Duration::from_millis(10000);
    thread::sleep(t);

    // Check invariants (all should commit)
    let truth = get_truth();
    for i in 0..n {
        assert!(check_history_same(i as u64, &truth), "failed at i = {}", i);
    }

    //assert!(1 == 2);

}
