use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;
use std::collections::HashMap;

fn main() {
    // create the node objects
    let mut nodes = Vec::new();
    let rnodes = &nodes;
    let N = 5;

    // leader
    nodes.push(Node::new(0, true));

    // followers
    for i in 1..N {
        nodes.push(Node::new(i, false));
    }

    // register tx channels for each node
    for i in 0..N as usize {
        for j in 0..N as usize {
            if i != j {
                let id = nodes[j].id;
                let sx = nodes[j].sx.clone();
                nodes[i].register(id, sx);
            }
        }
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
    fn new(i: i64, isLeader: bool) -> Node {
        let (s, r) = channel();
        Node {id: i, leader: isLeader, value: 0, tx: HashMap::new(), sx: s, rx: r}
    }

    fn register(&mut self, id: i64, tx: Sender<Message>) {
        match self.tx.insert(id, tx) {
            Some(v) => {
                println!("Error in register! value already present {:?}", v);
            },
            None => {}
        };
    }

    fn send(&self, id:i64, msg: Message) {
        println!("node {} sending {} to {}", self.id, msg.val, id);
        self.tx[&id].send(msg).unwrap();
        //println!("send successful");
    }
    fn receive(&self) -> Message {
        //println!("node {} receiving...", self.id);
        let m = self.rx.recv().unwrap();
        println!("node {} received {}", self.id, m.val);
        m
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
            println!("leader done!");
        } else {
            // follower
            let msg = self.receive();
            self.process(msg);
            self.send(0, Message {val: self.value});
            println!("follower done!");
        }
    }
}

enum MessageType {
    FollowerInfo(i64, String),
    Diff(i64),
    Trunc(i64),
    Snap(i64),
    ObserverInfo(i64),
    LeaderInfo(i64),
    AckEpoch(i64),
    NewLeader(i64),
    UpToDate,
    Proposal(i64),
    Ack(i64),
    Commit(i64),
    Inform(i64),
}

struct Message {
    val: i64,
}