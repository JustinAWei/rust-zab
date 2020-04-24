use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;
use std::collections::{HashSet, HashMap};

fn main() {
    // create the node objects
    let mut nodes = Vec::new();
    let rnodes = &nodes;
    let N: usize = 5;

    // leader
    nodes.push(Node::new(0, N, true));

    // followers
    for i in 1..N {
        nodes.push(Node::new(i, N, false));
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
            node.main_loop();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // wait until we terminate the program
    println!("Hello, world!");
}

struct InflightTxn {
    data: String,
    ack_ids: HashSet<usize>,
}

// node
struct Node {
    id: usize,
    cluster_size: usize,
    quorum_size: usize,
    leader: bool,
    committed_zxid: i64,
    latest_zxid: i64,
    tx: HashMap<usize, Sender<Message>>,
    sx: Sender<Message>,
    rx: Receiver<Message>,
    txn_log: Vec<(i64, String)>,
    inflight_txns: HashMap<i64, InflightTxn>,
}

impl Node {
    fn new(i: usize, cluster_size: usize, is_leader: bool) -> Node {
        assert!(cluster_size % 2 == 1);
        let (s, r) = channel();
        Node {
            id: i,
            cluster_size: cluster_size,
            quorum_size: (cluster_size + 1) / 2,
            leader: is_leader,
            committed_zxid: 0,
            latest_zxid: 0,
            tx: HashMap::new(),
            sx: s,
            rx: r,
            txn_log: Vec::new(),
            inflight_txns: HashMap::new(),
        }
    }

    fn register(&mut self, id: usize, tx: Sender<Message>) {
        match self.tx.insert(id, tx) {
            Some(v) => {
                println!("Error in register! value already present {:?}", v);
            },
            None => {}
        };
    }

    fn send(&self, id: usize, msg: Message) {
        println!("node {} sending {:?} to {}", self.id, msg, id);
        self.tx[&id].send(msg).unwrap();
        //println!("send successful");
    }
    fn receive(&self) -> Message {
        //println!("node {} receiving...", self.id);
        let m = self.rx.recv().unwrap();
        println!("node {} received {:?}", self.id, m);
        m
    }

    fn record_txn(&mut self, zxid: i64, data: String) {
        self.txn_log.push((zxid, data));
    }

    fn process(&mut self, msg:Message) {
        if self.leader {
            self.process_leader(msg);
        } else {
            self.process_follower(msg);
        }
    }

    fn process_leader(&mut self, msg: Message) {
        match msg.msg_type {
            MessageType::ClientProposal(data) => {
                self.latest_zxid += 1;
                let txn = InflightTxn {
                    data: data.clone(),
                    ack_ids: HashSet::new(),
                };
                self.inflight_txns.insert(self.latest_zxid, txn);
                let send_msg = Message {
                    sender_id: self.id,
                    msg_type: MessageType::Proposal(self.latest_zxid, data),
                };
                for id in 1..self.cluster_size {
                    self.send(id, send_msg.clone());
                }
                // TODO: spawn timeout
            },
            MessageType::Ack(zxid) => {
                match self.inflight_txns.get_mut(&zxid) {
                    Some(t) => {
                        t.ack_ids.insert(msg.sender_id);
                        if t.ack_ids.len() >= self.quorum_size {
                            // TODO handle quorum
                            println!("quorum!");
                        }
                    },
                    None => {},
                };
            },
            _ => {
                println!("Unsupported msg type for leader");
            }
        };
    }

    fn process_follower(&mut self, msg: Message) {
        // TODO this
    }

    fn main_loop(&mut self) {
        while true {
            let msg = self.receive();
            self.process(msg);
        }
        /*
        */
    }
}

#[derive(Clone, Debug)]
enum MessageType {
    // Zab message types
    /*
    FollowerInfo(i64, String),
    Diff(i64),
    Trunc(i64),
    Snap(i64),
    ObserverInfo(i64),
    LeaderInfo(i64),
    AckEpoch(i64),
    NewLeader(i64),
    UpToDate,
    */
    Proposal(i64, String),
    Ack(i64),
    Commit(i64),
    // Inform(i64),
    //
    // Our message types
    ClientProposal(String),
}

#[derive(Clone, Debug)]
struct Message {
    sender_id: usize,
    msg_type: MessageType,
}
