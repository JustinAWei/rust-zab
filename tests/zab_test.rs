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
static logpath_counter : AtomicU64 = AtomicU64::new(0);

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
    let my_id = logpath_counter.fetch_add(1, Ordering::SeqCst);
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
                let mut val : (u64, String) = serde_json::from_str(&ip).unwrap();
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
                let mut val : (String, u64, String) = serde_json::from_str(&ip).unwrap();
                if val.0 == "c" {
                    node_history.push((val.1, val.2));
                }
            }
        }
    }

    if node_history.len() != truth.len() {
        return false;
    }
    for i in 0..node_history.len() {
        if node_history[i] != truth[i] {
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
    let logpath = get_unique_logpath();
    let n = 5 as usize;
    let (senders, controller, mut handles, mut running, cl, ce) = start_up_nodes(n as u64, &logpath);
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
        assert!(check_history_same(i as u64, &logpath, &truth))
    }
    cleanup_logpath(logpath);
}

#[test]
// after nodes start up, one node should emerge as leader and other should follow
fn test_one_ldr_others_follow() {
    let logpath = get_unique_logpath();
    let n = 5 as usize;
    let (senders, controller, mut handles, mut running, cl, ce) = start_up_nodes(n as u64, &logpath);
    
    let t = time::Duration::from_millis(3000);
    thread::sleep(t);
    let curr_epoch = ce.load(Ordering::SeqCst);
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
    let (senders, controller, mut handles, mut running, cl, ce) = start_up_nodes(n as u64, &logpath);
    
    let t = time::Duration::from_millis(2000);
    thread::sleep(t);

    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("proposal00")),
    };
    for i in 0..1 {
        senders[&i].send(proposal.clone()).expect("nahh");
    }

    let t = time::Duration::from_millis(1000);
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