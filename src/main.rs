// use std::{thread, time};
pub mod election;
pub mod message;
pub mod comm;
pub mod zab_node;
// use zab_node::create_zab_ensemble;
// use message::{MessageType, Message, NodeState};
// use std::collections::{HashSet, HashMap};

fn main() {
    // println!("hello world!");
    // let nnodes = 21;
    // let (nodes, _senders, _controller) = create_zab_ensemble(nnodes);
    // let mut handles = Vec::new();
    // let curr_leader : Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    // let curr_epoch : Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    // let running : HashMap<u64, Arc<AtomicBool>> = HashMap::new();

    // // start them up
    // for (_node_id, mut node) in nodes {
    //     let curr_l = curr_leader.clone();
    //     let curr_e = curr_epoch.clone();
    //     let r = AtomicBool::new(true);
    //     running.insert(_node_id, r.clone());
    //     let handle = thread::spawn(move || {
    //         while r.load(Ordering::SeqCst) {
    //             node.main_loop();
    //             if node.epoch > curr_epoch && Some(node.id) == node.leader {
    //                 curr_l.store(node.id, Ordering::SeqCst);
    //                 curr_e.store(node.epoch, Ordering::SeqCst);
    //             }
    //         }
    //         return node;
    //     });
    //     handles.push(handle);
    // }
    
    // let proposal = Message {
    //     sender_id: 0,
    //     epoch: 1,
    //     msg_type: MessageType::ClientProposal(String::from("suhh")),
    // };

    // let t = time::Duration::from_millis(1000);
    // thread::sleep(t);

    // for i in 0..1 {
    //     // println!("client proposal!");
    //     _senders[&i].send(proposal.clone()).expect("nahh");
    // }

    // let t = time::Duration::from_millis(5000);
    // thread::sleep(t);

    // for i in 0..1 {
    //     // println!("client proposal again!");
    //     _senders[&i].send(proposal.clone()).expect("nahh");
    // }

    // let query = Message {
    //     sender_id: 0,
    //     epoch: 1,
    //     msg_type: MessageType::ClientQuery,
    // };

    // for (_, sender) in _senders {
    //     sender.send(query.clone()).expect("query failed :(");
    // }

    // for handle in handles {
    //     handle.join().unwrap();
    // }

    // // wait until we terminate the program
    // println!("Hello, world!");
}
