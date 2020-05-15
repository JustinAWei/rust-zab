// use rand::thread_rng;
use rand::seq::SliceRandom;
use std::{thread, time};
use zookeeper::zab_node::{Node, create_zab_ensemble};
use zookeeper::message::{MessageType, Message, NodeState};
use zookeeper::comm::{UnreliableSender, SenderController};
use std::collections::{HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::fs::{File};
use std::io::prelude::*;
// use std::io::BufReader;
use std::io;

static LOGPATH_COUNTER : AtomicU64 = AtomicU64::new(0);
const results_filename   : &str = "logs/results.log";
const SLP_PROPOSAL_MS           : u64 = 2000;
const SLP_PROPOSAL_TIMEOUT_MS   : u64 = 2500;
const SLP_LDR_ELECT5            : u64 = 3500;
const SLP_STRAGGLER_CATCHUP     : u64 = 2500;
const BAD_ZXID_PROPOSAL         : u64 = 0x0fffffffffff0000;

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines(filename: &str) -> io::Result<io::Lines<io::BufReader<File>>> {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

// returns sender, controller, handles, running, arc to c. leader, arc to c. epoch
fn start_up_nodes(nnodes : u64, log_base : &String) 
    -> (HashMap<u64, mpsc::Sender<Message>>,
        SenderController,
        HashMap<u64, JoinHandle<Node<UnreliableSender<Message>>>>,
        HashMap<u64, Arc<AtomicBool>>,
        Arc<AtomicU64>,
        Arc<AtomicU64>)
    {
    let (nodes, _senders, _controller) = create_zab_ensemble(nnodes, log_base);
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
                    println!("Epoch and leader advanced! {} {}", node.epoch, node.id);
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

fn restart_node(oldnode : Node<UnreliableSender<Message>>,
    handles : & mut HashMap<u64, JoinHandle<Node<UnreliableSender<Message>>>>,
    senders : &HashMap<u64, mpsc::Sender<Message>>,
    running : & HashMap<u64, Arc<AtomicBool>>,
    curr_leader : & Arc<AtomicU64>,
    curr_epoch : & Arc<AtomicU64>) 
{
    let node_id = oldnode.id;
    let mut node = Node::new_from(oldnode, senders.get(&node_id).unwrap().clone());
    let curr_l = curr_leader.clone();
    let curr_e = curr_epoch.clone();
    let r = running.get(&node_id).unwrap().clone();
    r.store(true, Ordering::SeqCst);
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
    handles.insert(node_id, handle);
}


fn kill(node_id : u64,
        senders : & HashMap<u64, mpsc::Sender<Message>>,
        handles : & mut HashMap<u64, JoinHandle<Node<UnreliableSender<Message>>>>,
        running : & mut HashMap<u64, Arc<AtomicBool>>) -> Node<UnreliableSender<Message>>
{
    running.get_mut(&node_id).unwrap().store(false, Ordering::SeqCst);
    let return_to_main = Message {
                    sender_id: 0,
                    epoch: 0,
                    msg_type: MessageType::ReturnToMainloop,
                };
    senders.get(&node_id).unwrap().send(return_to_main.clone()).unwrap();
    let result = handles.remove(&node_id).unwrap().join().unwrap();
    return result;
}

fn get_unique_logpath() -> String {
    let my_id = LOGPATH_COUNTER.fetch_add(1, Ordering::SeqCst);
    let logpath = format!("testslogs{}", my_id);
    cleanup_logpath(logpath.clone());
    return logpath;
}

fn cleanup_logpath(logpath : String) {
    std::fs::remove_dir_all(logpath.clone());
    std::fs::remove_dir(logpath);
}

fn get_truth(log_base : &String) -> Vec<(u64, String)> {
    // Open the file in read-only mode with buffer.
    let mut results_history : Vec<(u64, String)>  = Vec::new();
    if let Ok(lines) = read_lines(&format!("./{}/results.log", log_base)) {
        // Consumes the iterator, returns an (Optional) String
        for line in lines {
            if let Ok(ip) = line {
                let val : (u64, String) = serde_json::from_str(&ip).unwrap();
                results_history.push(val);
            }
        }
    }
    return results_history;
}

fn check_history_same(node_id : u64, log_base : &String, truth: &Vec<(u64, String)>) -> bool {
    let mut node_history : Vec<(u64, String)>  = Vec::new();
    if let Ok(lines) = read_lines(&format!("./{}/{}.log", log_base, node_id)) {
        // Consumes the iterator, returns an (Optional) String
        for line in lines {
            if let Ok(ip) = line {
                let val : (String, u64, String) = serde_json::from_str(&ip).unwrap();
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

fn check_history_prefix(node_id : u64, log_base : &String, truth: &Vec<(u64, String)>) -> bool {
    let mut node_history : Vec<(u64, String)>  = Vec::new();
    if let Ok(lines) = read_lines(&format!("./{}/{}.log", log_base, node_id)) {
        // Consumes the iterator, returns an (Optional) String
        for line in lines {
            if let Ok(ip) = line {
                let val : (String, u64, String) = serde_json::from_str(&ip).unwrap();
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
    let logpath = get_unique_logpath();
    let n = 5 as usize;
    let (senders, _controller, mut handles, mut running, _cl, _ce) = start_up_nodes(n as u64, &logpath);
    assert!(senders.len() == n);
    assert!(handles.len() == n);
    assert!(running.len() == n);

    // enough time for nodes to begin running
    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);

    for i in 0..n {
        kill(i as u64, & senders, & mut handles, & mut running);
        assert!(handles.len() == n - i - 1);
    
    }
    let truth : Vec<(u64, String)> = Vec::new();

    for i in 0..n {
        assert!(check_history_same(i as u64, &logpath, &truth))
    }
    cleanup_logpath(logpath);
}

#[test]
// after nodes start up, one node should emerge as leader and other should follow
fn test_one_ldr_others_follow() {
    let logpath = get_unique_logpath();
    let n = 5 as usize;
    let (senders, _controller, mut handles, mut running, cl, _ce) = start_up_nodes(n as u64, &logpath);
    
    let t = time::Duration::from_millis(SLP_LDR_ELECT5);
    thread::sleep(t);
    // let curr_epoch = ce.load(Ordering::SeqCst);
    let curr_ldr = cl.load(Ordering::SeqCst);

    let node = kill(curr_ldr, & senders, & mut handles, & mut running);
    assert_eq!(node.state, NodeState::Leading);

    for i in 0..n {
        if i as u64 != curr_ldr {
            let node = kill(i as u64, & senders, & mut handles, & mut running);
            assert_eq!(node.state, NodeState::Following)
        }
    }
    assert!(handles.is_empty());
    thread::sleep(t);
    cleanup_logpath(logpath);
}

#[test]
fn test_stable_one_proposal() {
    let logpath = get_unique_logpath();
    let n = 5 as usize;
    let (senders, _controller, mut handles, mut running, _cl, _ce) = start_up_nodes(n as u64, &logpath);
    
    let t = time::Duration::from_millis(SLP_LDR_ELECT5);
    thread::sleep(t);

    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal00")),
    };
    for i in 0..1 {
        senders[&i].send(proposal.clone()).expect("nahh");
    }

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);
    
    for i in 0..n {
        kill(i as u64, & senders, & mut handles, & mut running);
    }
    let truth = get_truth(&logpath);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }
    cleanup_logpath(logpath);
}

#[test]
fn test_kill_1_follower_nohist_generic() {
    test_kill_some_follower_nohist(5, 1);
}

#[test]
fn test_kill_2_follower_nohist() {
    test_kill_some_follower_nohist(5, 2);
}

#[test]
fn test_kill_3_follower_nohist() {
    test_kill_some_follower_nohist(7, 3);
}

#[test]
fn test_kill_4_follower_nohist() {
    test_kill_some_follower_nohist(9, 4);
}

#[test]
fn test_kill_5_follower_nohist() {
    test_kill_some_follower_nohist(11, 5);
}

fn test_kill_some_follower_nohist(n: u64, m : u64) {
    let logpath = get_unique_logpath();
    let (senders, _, mut handles, mut running, cl, ce) = start_up_nodes(n as u64, &logpath);
    
    let t = time::Duration::from_millis(SLP_LDR_ELECT5);
    thread::sleep(t);

    let curr_ldr = cl.load(Ordering::SeqCst);

    let mut killed: HashMap<u64, Option<Node<UnreliableSender<Message>>>> = HashMap::new();

    // kill node
    let mut killstreak = 0;
    for i in 0..n {
        if i != curr_ldr {
            let node = kill(i as u64, & senders, & mut handles, & mut running);
            killed.insert(node.id, Some(node));
            killstreak += 1;
            if killstreak == m {break;}
        }
    }

    // make proposal, this should be committed to all other nodes
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal01")),
    };
    senders[&curr_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);
    
    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 1);
    for i in 0..n {
        if killed.contains_key(&i) {
            assert!(check_history_prefix(i as u64, &logpath, &truth));
            // history shouldn't be same
            assert!(!check_history_same(i as u64, &logpath, &truth));
        } else {
            assert!(check_history_same(i as u64, &logpath, &truth));
        }
    }

    for i in 0..n {
        if killed.contains_key(&i) {
            restart_node(killed.remove(&i).unwrap().unwrap(), &mut handles, &senders, & running, &cl, &ce);
        }
    }

    let t = time::Duration::from_millis(SLP_STRAGGLER_CATCHUP);
    thread::sleep(t);

    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 1);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }
    // make proposal, this should be committed to all nodes, including the 
    //  one that was killed and reconnected
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal02")),
    };
    senders[&curr_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);

    for i in 0..n {
        kill(i as u64, & senders, & mut handles, & mut running);
    }

    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 2);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }
    cleanup_logpath(logpath);
}

#[test]
fn test_kill_1_follower_nohist() {
    let logpath = get_unique_logpath();
    let n : u64 = 5 ;
    let (senders, _controller, mut handles, mut running, cl, ce) = start_up_nodes(n as u64, &logpath);
    
    let t = time::Duration::from_millis(SLP_LDR_ELECT5);
    thread::sleep(t);

    let curr_ldr = cl.load(Ordering::SeqCst);
    // kill one node
    let mut killed_node = None;
    for i in 0..n {
        if i != curr_ldr {
            let node = kill(i as u64, & senders, & mut handles, & mut running);
            killed_node = Some(node);
            break;
        }
    }

    // make proposal, this should be committed to all other nodes
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal01")),
    };
    senders[&curr_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);
    
    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 1);
    for i in 0..n {
        if i == (&killed_node).as_ref().unwrap().id {
            assert!(check_history_prefix(i as u64, &logpath, &truth));
            // history shouldn't be same
            assert!(!check_history_same(i as u64, &logpath, &truth));
        } else {
            assert!(check_history_same(i as u64, &logpath, &truth));
        }
    }

    restart_node(killed_node.unwrap(), &mut handles, &senders, & running, &cl, &ce);

    let t = time::Duration::from_millis(SLP_STRAGGLER_CATCHUP);
    thread::sleep(t);

    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 1);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }
    // make proposal, this should be committed to all nodes, including the 
    //  one that was killed and reconnected
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal02")),
    };
    senders[&curr_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);

    for i in 0..n {
        kill(i as u64, & senders, & mut handles, & mut running);
    }

    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 2);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }
    cleanup_logpath(logpath);
}

#[test]
fn test_kill_1_follower_prevhist() {
    let logpath = get_unique_logpath();
    let n : u64 = 5 ;
    let (senders, _controller, mut handles, mut running, cl, ce) = start_up_nodes(n as u64, &logpath);
    
    let t = time::Duration::from_millis(SLP_LDR_ELECT5);
    thread::sleep(t);

    let curr_ldr = cl.load(Ordering::SeqCst);
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal00")),
    };
    senders[&curr_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);
    
    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 1);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }

    // kill one node
    let mut killed_node = None;
    for i in 0..n {
        if i != curr_ldr {
            let node = kill(i as u64, & senders, & mut handles, & mut running);
            killed_node = Some(node);
            break;
        }
    }

    // make proposal, this should be committed to all other nodes
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal01")),
    };
    senders[&curr_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);
    
    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 2);
    for i in 0..n {
        if i == (&killed_node).as_ref().unwrap().id {
            assert!(check_history_prefix(i as u64, &logpath, &truth));
            // history shouldn't be same
            assert!(!check_history_same(i as u64, &logpath, &truth));
        } else {
            assert!(check_history_same(i as u64, &logpath, &truth));
        }
    }

    restart_node(killed_node.unwrap(), &mut handles, &senders, & running, &cl, &ce);

    let t = time::Duration::from_millis(SLP_STRAGGLER_CATCHUP);
    thread::sleep(t);

    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 2);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }
    // make proposal, this should be committed to all nodes, including the 
    //  one that was killed and reconnected
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal02")),
    };
    senders[&curr_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);

    for i in 0..n {
        kill(i as u64, & senders, & mut handles, & mut running);
    }

    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 3);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }
    cleanup_logpath(logpath);
}

#[test]
fn test_kill_1_follower_prevhist_generic() {
    test_kill_some_follower_prevhist(5, 1);
}

#[test]
fn test_kill_2_follower_prevhist() {
    test_kill_some_follower_prevhist(5, 2);
}

#[test]
fn test_kill_3_follower_prevhist() {
    test_kill_some_follower_prevhist(7, 3);
}

#[test]
fn test_kill_4_follower_prevhist() {
    test_kill_some_follower_prevhist(9, 4);
}

#[test]
fn test_kill_5_follower_prevhist() {
    test_kill_some_follower_prevhist(11, 5);
}

fn test_kill_some_follower_prevhist(n: u64, m : u64) {
    let logpath = get_unique_logpath();
    let (senders, _, mut handles, mut running, cl, ce) = start_up_nodes(n as u64, &logpath);
    
    let t = time::Duration::from_millis(SLP_LDR_ELECT5);
    thread::sleep(t);

    let curr_ldr = cl.load(Ordering::SeqCst);
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal00")),
    };

    senders[&curr_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);
    
    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 1);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }

    let mut killed: HashMap<u64, Option<Node<UnreliableSender<Message>>>> = HashMap::new();

    // kill node
    let mut killstreak = 0;
    for i in 0..n {
        if i != curr_ldr {
            let node = kill(i as u64, & senders, & mut handles, & mut running);
            killed.insert(node.id, Some(node));
            killstreak += 1;
            if killstreak == m {break;}
        }
    }

    // make proposal, this should be committed to all other nodes
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal01")),
    };
    senders[&curr_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);
    
    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 2);
    for i in 0..n {
        if killed.contains_key(&i) {
            assert!(check_history_prefix(i as u64, &logpath, &truth));
            // history shouldn't be same
            assert!(!check_history_same(i as u64, &logpath, &truth));
        } else {
            assert!(check_history_same(i as u64, &logpath, &truth));
        }
    }

    for i in 0..n {
        if killed.contains_key(&i) {
            restart_node(killed.remove(&i).unwrap().unwrap(), &mut handles, &senders, & running, &cl, &ce);
        }
    }

    let t = time::Duration::from_millis(SLP_STRAGGLER_CATCHUP);
    thread::sleep(t);

    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 2);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }
    // make proposal, this should be committed to all nodes, including the 
    //  one that was killed and reconnected
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal02")),
    };
    senders[&curr_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);

    for i in 0..n {
        kill(i as u64, & senders, & mut handles, & mut running);
    }

    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 3);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }
    cleanup_logpath(logpath);
}

#[test]
fn test_kill_1_roundrobin() {
    let logpath = get_unique_logpath();
    let n : u64 = 5 ;
    let (senders, _controller, mut handles, mut running, cl, ce) = start_up_nodes(n as u64, &logpath);
    
    // wait until stable state
    let t = time::Duration::from_millis(SLP_LDR_ELECT5);
    thread::sleep(t);

    let mut curr_ldr = cl.load(Ordering::SeqCst);
    let mut curr_epoch = ce.load(Ordering::SeqCst);
    // kill one node
    for i in 0..n {
        let killed_node = kill(i as u64, & senders, & mut handles, & mut running);

        if i == curr_ldr {
            // may have killed a leader
            // make proposal, this should be sent to all nodes s.t. they time out
            let proposal = Message {
                sender_id: curr_ldr,
                epoch: curr_epoch,
                msg_type: MessageType::Proposal(BAD_ZXID_PROPOSAL, String::from("proposal00")),
            };
            for i in 0..n {
                if i != curr_ldr {
                    senders[&i].send(proposal.clone()).expect("nahh");
                }
            }
            let t = time::Duration::from_millis(SLP_LDR_ELECT5);
            thread::sleep(t);
                    
            let new_ldr = cl.load(Ordering::SeqCst);
            assert!(curr_ldr != new_ldr);
            let new_epoch = ce.load(Ordering::SeqCst);
            assert!(new_epoch > curr_epoch, "{} !> {}", new_epoch, curr_epoch);
            curr_ldr = new_ldr;
            curr_epoch = new_epoch;
        }

        // make proposal, this should be committed to all other nodes
        let proposal = Message {
            sender_id: 0,
            epoch: 1,
            msg_type: MessageType::ClientProposal(String::from("proposal")),
        };
        senders[&curr_ldr].send(proposal.clone()).expect("nahh");
        
        let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
        thread::sleep(t);

        let truth = get_truth(&logpath);
        assert_eq!(truth.len() as u64, i + 1);
        for i in 0..n {
            if i == (&killed_node).id {
                assert!(check_history_prefix(i as u64, &logpath, &truth));
                // history shouldn't be same
                assert!(!check_history_same(i as u64, &logpath, &truth));
            } else {
                assert!(check_history_same(i as u64, &logpath, &truth));
            }
        }
        restart_node(killed_node, &mut handles, &senders, & running, &cl, &ce);

        let t = time::Duration::from_millis(SLP_STRAGGLER_CATCHUP);
        thread::sleep(t);
        let truth = get_truth(&logpath);
        assert_eq!(truth.len() as u64, i + 1);
        for i in 0..n {
            check_history_same(i as u64, &logpath, &truth);
        }
    }

    
    cleanup_logpath(logpath);
}

#[test]
fn test_kill_leader_nohist() {
    let logpath = get_unique_logpath();
    let n : u64 = 5 ;
    let (senders, _controller, mut handles, mut running, cl, ce) = start_up_nodes(n as u64, &logpath);
    
    let t = time::Duration::from_millis(SLP_LDR_ELECT5);
    thread::sleep(t);

    let old_e = ce.load(Ordering::SeqCst);
    let old_ldr = cl.load(Ordering::SeqCst);
    // kill one node
    let killed_node = kill(old_ldr, & senders, & mut handles, & mut running);

    // make proposal, this should be sent to all nodes s.t. they time out
    let proposal = Message {
        sender_id: old_ldr,
        epoch: old_e,
        msg_type: MessageType::Proposal(BAD_ZXID_PROPOSAL, String::from("proposal00")),
    };
    for i in 0..n {
        if i != old_ldr {
            senders[&i].send(proposal.clone()).expect("nahh");
        }
    }

    // wait until new leader is elected
    
    let t = time::Duration::from_millis(SLP_LDR_ELECT5 + SLP_PROPOSAL_TIMEOUT_MS);
    thread::sleep(t);
    let new_ldr = cl.load(Ordering::SeqCst);
    let new_e = ce.load(Ordering::SeqCst);

    assert!(new_ldr != old_ldr);
    assert!(new_e > old_e);

    // make proposal, this should be committed to all other nodes
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal00")),
    };
    senders[&new_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);
    
    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 1);
    for i in 0..n {
        if i == (&killed_node).id {
            assert!(check_history_prefix(i as u64, &logpath, &truth));
            // history shouldn't be same
            assert!(!check_history_same(i as u64, &logpath, &truth));
        } else {
            assert!(check_history_same(i as u64, &logpath, &truth));
        }
    }

    restart_node(killed_node, &mut handles, &senders, & running, &cl, &ce);

    let t = time::Duration::from_millis(SLP_STRAGGLER_CATCHUP);
    thread::sleep(t);

    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 1);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }
    // make proposal, this should be committed to all nodes, including the 
    //  one that was killed and reconnected
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal01")),
    };
    senders[&new_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);
    // leader and epoch should have stayed the same
    assert_eq!(new_ldr, cl.load(Ordering::SeqCst));
    assert_eq!(new_e, ce.load(Ordering::SeqCst));

    for i in 0..n {
        let n = kill(i as u64, & senders, & mut handles, & mut running);
        if n.id == new_ldr {
            assert_eq!(n.state, NodeState::Leading);
        } else {
            assert_eq!(n.state, NodeState::Following);
        }
    }

    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 2);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }
    cleanup_logpath(logpath);
}

#[test]
fn test_kill_leader_prevhist() {
    let logpath = get_unique_logpath();
    let n : u64 = 5 ;
    let (senders, _controller, mut handles, mut running, cl, ce) = start_up_nodes(n as u64, &logpath);
    
    let t = time::Duration::from_millis(SLP_LDR_ELECT5);
    thread::sleep(t);

    let old_e = ce.load(Ordering::SeqCst);
    let old_ldr = cl.load(Ordering::SeqCst);
    // make proposal, this should be committed to all nodes
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal00")),
    };
    senders[&old_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);
    
    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 1);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }

    // kill one node
    let killed_node = kill(old_ldr, & senders, & mut handles, & mut running);

    // make proposal, this should be sent to all nodes s.t. they time out
    let proposal = Message {
        sender_id: old_ldr,
        epoch: old_e,
        msg_type: MessageType::Proposal(BAD_ZXID_PROPOSAL, String::from("proposal00")),
    };
    for i in 0..n {
        if i != old_ldr {
            senders[&i].send(proposal.clone()).expect("nahh");
        }
    }

    // wait until new leader is elected
    
    let t = time::Duration::from_millis(SLP_LDR_ELECT5 + SLP_PROPOSAL_TIMEOUT_MS);
    thread::sleep(t);
    let new_ldr = cl.load(Ordering::SeqCst);
    let new_e = ce.load(Ordering::SeqCst);

    assert!(new_ldr != old_ldr);
    assert!(new_e > old_e);

    // make proposal, this should be committed to all other nodes
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal00")),
    };
    senders[&new_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);
    
    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 2);
    for i in 0..n {
        if i == (&killed_node).id {
            assert!(check_history_prefix(i as u64, &logpath, &truth));
            // history shouldn't be same
            assert!(!check_history_same(i as u64, &logpath, &truth));
        } else {
            assert!(check_history_same(i as u64, &logpath, &truth));
        }
    }

    restart_node(killed_node, &mut handles, &senders, & running, &cl, &ce);

    let t = time::Duration::from_millis(SLP_STRAGGLER_CATCHUP);
    thread::sleep(t);

    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 2);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }
    // make proposal, this should be committed to all nodes, including the 
    //  one that was killed and reconnected
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal01")),
    };
    senders[&new_ldr].send(proposal.clone()).expect("nahh");

    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);
    // leader and epoch should have stayed the same
    assert_eq!(new_ldr, cl.load(Ordering::SeqCst));
    assert_eq!(new_e, ce.load(Ordering::SeqCst));

    for i in 0..n {
        let n = kill(i as u64, & senders, & mut handles, & mut running);
        if n.id == new_ldr {
            assert_eq!(n.state, NodeState::Leading);
        } else {
            assert_eq!(n.state, NodeState::Following);
        }
    }

    let truth = get_truth(&logpath);
    assert_eq!(truth.len(), 3);
    for i in 0..n {
        check_history_same(i as u64, &logpath, &truth);
    }
    cleanup_logpath(logpath);
}

#[test]
fn network_partition_followers() {
    // Setup
    let logpath = get_unique_logpath();
    let n = 5 as usize;
    let (senders, controller, mut handles, mut running, cl, _ce) = start_up_nodes(n as u64, &logpath);
    assert!(senders.len() == n);
    assert!(handles.len() == n);
    assert!(running.len() == n);

    let t = time::Duration::from_millis(SLP_LDR_ELECT5);
    thread::sleep(t);

    // Initial proposal
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("suhh")),
    };
    senders[&0].send(proposal.clone()).expect("nahh");
    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);

    // Check invariants
    let truth = get_truth(&logpath);
    for i in 0..n {
        assert!(check_history_same(i as u64, &logpath, &truth), "failed at i = {}", i);
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
    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);

    // Check invariants (no new commits)
    let truth = get_truth(&logpath);
    for i in 0..n {
        assert!(check_history_same(i as u64, &logpath, &truth), "failed at i = {}", i);
    }

    // Propose to quorum
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("suhh1")),
    };
    senders[&cl.load(Ordering::SeqCst)].send(proposal.clone()).expect("nahh");
    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);

    // Check invariants (quorum should have committed, partition should not have)
    let mut same_count = 0;
    let truth = get_truth(&logpath);
    for i in 0..n {
        assert!(check_history_prefix(i as u64, &logpath, &truth), "failed at i = {}", i);
        if check_history_same(i as u64, &logpath, &truth) {
            same_count += 1;
        }
    }
    assert!(same_count == n - p_size, "{} != {} - {}", same_count, n, p_size);

    // Restore partition and send proposal
    for i in 0..n {
        for j in 0..n {
            controller.make_sender_ok(i as u64, j as u64);
            controller.make_sender_ok(j as u64, i as u64);
        }
    }
    let t = time::Duration::from_millis(SLP_PROPOSAL_MS);
    thread::sleep(t);
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("suhh2")),
    };
    senders[&cl.load(Ordering::SeqCst)].send(proposal.clone()).expect("nahh");
    let t = time::Duration::from_millis(SLP_LDR_ELECT5);
    thread::sleep(t);
    
    for i in 0..n {
        kill(i as u64, & senders, & mut handles, & mut running);
    }
    

    // Check invariants (all should commit)
    let truth = get_truth(&logpath);
    for i in 0..n {
        assert!(check_history_same(i as u64, &logpath, &truth), "failed at i = {}", i);
    }
    cleanup_logpath(logpath);
}
