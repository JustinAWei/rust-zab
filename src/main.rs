use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;
use std::collections::HashMap;

fn main() {
    // create the node objects
    let mut nodes = Vec::new();
    let rnodes = &nodes;
    let N = 5;

    // leader
    nodes.push(Node::new(0));

    // followers
    for i in 1..N {
        nodes.push(Node::new(i));
    }
    // &rnodes[0].tx.insert(1, nodes[1].sx.clone());

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
    sx: Sender<Message>,
    rx: Receiver<Message>,
}

impl Node {
    fn new(i: i64, ) -> Node {
        let (s, r) = channel();
        Node {id: i, leader: false, value: 0, tx: HashMap::new(), sx: s, rx: r}
    }
    fn send(&self, id:i64, msg: Message) {
        self.tx[&id].send(msg).unwrap();
    }
    fn receive(&self) -> Message {
        self.rx.recv().unwrap()
    }

    fn process(&mut self, msg:Message) {
        self.value = msg.val;
    }

    fn operate(&mut self) {
        if self.leader {
            // leader
            self.value += 1;
            for id in 1..5 {
                self.send(id, Message {val: self.value});
            }
            for _ in 1..5 {
                let ack = self.receive();
                assert!(ack.val == self.value);
            }
        } else {
            // follower
            for i in 0..3 {
                let msg = self.receive();
                self.process(msg);
                self.send(0, Message {val: self.value});
            }
        }
    }
}

struct Message {
    val: i64
}