use std::sync::mpsc::{Sender, Receiver};
use std::thread;
use std::collections::HashMap;

fn main() {
    // create the node objects
    let mut nodes = Vec::new();
    let N = 5;

    // leader
    nodes.push(Node {id: 0, leader: true, value: 0, tx: HashMap::new(), rx: HashMap::new()});

    // followers
    for i in 1..N {
        nodes.push(Node {id: i, leader: false, value: 0, tx: HashMap::new(), rx: HashMap::new()});
    }

    let mut handles = Vec::new();
    // start them up
    for mut node in nodes {
        let handle = thread::spawn(move || {
            node.operate();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
    
    // wait until we terminate the program
    println!("Hello, world!");
}

// node
struct Node {
    id: i64,
    leader: bool,
    value: i64,
    tx: HashMap<i64, Sender<Message> >,
    rx: HashMap<i64, Receiver<Message> >
}

impl Node {
    fn send(&self, id:i64, msg: Message) {
        self.tx[&id].send(msg).unwrap();
    }
    fn receive(&self, id: i64) -> Message {
        self.rx[&id].recv().unwrap()
    }

    fn process(&mut self, msg:Message) {
        self.value = msg.val;
    }

    fn operate(&mut self) {
        if self.id == 0 {
            // leader
            self.value += 1;
            for id in 1..5 {
                self.send(id, Message {val: self.value});
            }
            for id in 1..5 {
                let ack = self.receive(id);
                assert!(ack.val == self.value);
            }
        } else {
            // follower
            for i in 0..3 {
                let msg = self.receive(0);
                self.process(msg);
                self.send(0, Message {val: self.value});
            }
        }
    }
}

struct Message {
    val: i64
}