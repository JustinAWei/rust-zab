use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;
use std::collections::{HashSet, HashMap};
extern crate timer;
extern crate chrono;
const TXN_TIMEOUT_MS : i64 = 400;

fn main() {
    // create the node objects
    let mut nodes = Vec::new();
    let n: usize = 5;

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
}

struct InflightTxn {
    data: String,
    ack_ids: HashSet<usize>,
    scheduler_handle: timer::Guard
}

// node
struct Node {
    id: usize,
    cluster_size: usize,
    quorum_size: usize,
    leader: bool,
    committed_zxid: i64,
    next_zxid: i64,
    tx: HashMap<usize, Sender<Message>>,
    sx: Sender<Message>,
    rx: Receiver<Message>,
    commit_log: Vec<(i64, String)>, // TODO: timestamp?
    proposal_log: Vec<(i64, String)>, // TODO: timestamp?
    inflight_txns: HashMap<i64, InflightTxn>,
    msg_thread: timer::MessageTimer<Message>,
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
            next_zxid: 1,
            tx: HashMap::new(),
            sx: s.clone(),
            rx: r,
            commit_log: Vec::new(),
            proposal_log: Vec::new(),
            inflight_txns: HashMap::new(),
            msg_thread: timer::MessageTimer::new(s)
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

    fn record_proposal(&mut self, zxid: i64, data: String) {
        self.proposal_log.push((zxid, data));
    }

    fn record_commit(&mut self, zxid: i64, data: String) {
        self.commit_log.push((zxid, data));
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
                let zxid = self.next_zxid;
                self.next_zxid += 1;
                self.record_proposal(zxid, data.clone()); // TODO: beginning or end of msg handler?

                let mut txn = InflightTxn {
                    data: data.clone(),
                    ack_ids: HashSet::new(),
                    scheduler_handle: self.spawnTimeout(zxid)
                };
                txn.ack_ids.insert(self.id);

                self.inflight_txns.insert(zxid, txn);
                let send_msg = Message {
                    sender_id: self.id,
                    msg_type: MessageType::Proposal(zxid, data),
                };
                for id in 0..self.cluster_size {
                    if id != self.id {
                        self.send(id, send_msg.clone());
                    }
                }
            },
            MessageType::Ack(zxid) => {
                let mut quorum_ack : bool = false;
                match self.inflight_txns.get_mut(&zxid) {
                    Some(t) => {
                        t.ack_ids.insert(msg.sender_id);
                        if t.ack_ids.len() >= self.quorum_size {
                            quorum_ack = true
                        }
                    },
                    None => {},
                };

                if quorum_ack {
                    if zxid != self.committed_zxid + 1 {
                        panic!("leader missed a zxid");
                        // return;
                    }
                    match self.inflight_txns.remove(&zxid) {
                        Some(t) => {
                            // handle quorum
                            // we can first send to followers before writing in our own logs
                            let send_msg = Message {
                                sender_id: self.id,
                                msg_type: MessageType::Commit(zxid),
                            };
                            for id in 0..self.cluster_size {
                                if id != self.id {
                                    self.send(id, send_msg.clone());
                                }
                            }
                            // TODO: where do we record??
                            self.record_commit(zxid, t.data.clone());
                            self.committed_zxid = zxid;
                        },
                        None => {}
                    }
                }
            },
            _ => {
                println!("Unsupported msg type for leader");
            }
        };
    }

    fn process_follower(&mut self, msg: Message) {
        let leader_id = msg.sender_id;
        match msg.msg_type {
            MessageType::Proposal(zxid, data) => {
                if zxid != self.next_zxid {
                    println!("follower missed a zxid");
                    return;
                }
                self.next_zxid += 1;

                let txn = InflightTxn {
                    data: data.clone(),
                    ack_ids: HashSet::new(), // TODO null?
                    scheduler_handle: self.spawnTimeout(zxid)
                };
                self.inflight_txns.insert(zxid, txn);

                // send ACK(zxid) to the great leader.
                let ack = Message {
                    sender_id: self.id,
                    msg_type: MessageType::Ack(zxid)
                };
                self.send(leader_id, ack);

                self.record_proposal(zxid, data);
            },
            MessageType::Commit(zxid) => {
                if zxid != self.committed_zxid + 1 {
                    println!("follower missed a zxid");
                    return;
                }

                match self.inflight_txns.remove(&zxid) {
                    Some(t) => {
                        self.record_commit(zxid, t.data.clone());
                        self.committed_zxid = zxid;
                    },
                    None => {}
                }
            },
            _ => {
                println!("Unsupported msg type for follower");
            }
        }
    }

    fn main_loop(&mut self) {
        loop {
            let msg = self.receive();
            self.process(msg);
        }
        /*
        */
    }

    fn spawnTimeout(&self, zxid: i64) -> timer::Guard {
        self.msg_thread.schedule_with_delay(
            chrono::Duration::milliseconds(TXN_TIMEOUT_MS),
            Message {
                sender_id: self.id,
                // TODO handle this message
                msg_type: MessageType::InternalTimeout(
                    zxid
                )
            }
        )
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
    InternalTimeout(i64) // zxid of timed out transaction
}

#[derive(Clone, Debug)]
struct Message {
    sender_id: usize,
    msg_type: MessageType,
}
