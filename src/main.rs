use std::{thread, time};
pub mod election;
pub mod message;
pub mod comm;
pub mod zab_node;
use zab_node::create_zab_ensemble;
use message::{MessageType, Message, NodeState};

fn main() {
    println!("hello world!");
    let nnodes = 11;
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

    let t = time::Duration::from_millis(5000);
    thread::sleep(t);

    for i in 0..11 {
        println!("client proposal!");
        _senders[&i].send(proposal.clone()).expect("nahh");
    }

    thread::sleep(t);

    let query = Message {
        sender_id: 0,
        epoch: 1,
        msg_type: MessageType::ClientQuery,
    };

    for (_, sender) in _senders {
        sender.send(query.clone()).expect("query failed :(");
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // wait until we terminate the program
    println!("Hello, world!");
}
