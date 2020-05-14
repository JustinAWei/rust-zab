use std::sync::mpsc::{Sender, Receiver, channel, RecvTimeoutError};
use std::thread;
use std::collections::{HashSet, HashMap};
// extern crate serde;
extern crate serde_json;
pub mod election;
pub mod message;
pub mod comm;
pub mod zab_node;
use zab_node::Node;
use message::{MessageType, Message};

fn main() {
    println!("hello world!");
    /*
    // create the node objects
    let mut nodes = Vec::new();
    let n: u64 = 5;

    let mut sx : HashMap< = HashMap::new();

    // leader
    nodes.push(Node::new(0, n, true));
    let leader_channel = nodes[0].sx.clone();

    // followers
    for i in 1..n {
        nodes.push(Node::new(i, n, false));
    }

    // register tx channels for each node
    for i in 0..n as usize {
        for j in 0..n as usize {
            if i != j {
                let id = nodes[j].id;
                let sx = nodes[j].sx.clone();
                nodes[i].register(id, sx);
            }
        }
    }

    let mut handles = Vec::new();
    // start them up
    for mut node in nodes {
        let handle = thread::spawn(move || {
            node.main_loop();
        });
        handles.push(handle);
    }

    let proposal = Message {
        sender_id: 0,
        epoch: 0,
        msg_type: MessageType::ClientProposal(String::from("suhh")),
    };
    for _ in 0..5 {
        leader_channel.send(proposal.clone()).expect("nahh");
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // wait until we terminate the program
    println!("Hello, world!");
*/
}