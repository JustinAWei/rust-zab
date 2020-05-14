use std::thread;
pub mod election;
pub mod message;
pub mod comm;
pub mod zab_node;
use zab_node::create_zab_ensemble;
use message::{MessageType, Message, NodeState};

fn main() {
    println!("hello world!");
    let nnodes = 5;
    let (nodes, _senders, _controller) = create_zab_ensemble(nnodes);
    let mut handles = Vec::new();

    // start them up
    for (_node_id, mut node) in nodes {
        let handle = thread::spawn(move || {
            node.main_loop();
        });
        handles.push(handle);
    }
    
    let proposal = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientProposal(String::from("suhh")),
    };

    for _ in 0..100000000 {
    }
    for i in 0..1 {
        println!("client proposal!");
        _senders[&i].send(proposal.clone()).expect("nahh");
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // wait until we terminate the program
    println!("Hello, world!");
}