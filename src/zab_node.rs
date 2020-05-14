use std::time::{Duration, Instant};
use std::fs::File;
use std::io::prelude::*;
use std::sync::mpsc::{Sender, Receiver, channel, RecvTimeoutError};
use std::collections::{HashSet, HashMap};
use timer;
use chrono;
use crate::election::LeaderElector;
use crate::message::{MessageType, Message, NodeState};
use crate::comm::{BaseSender, UnreliableSender, SenderController};

const TXN_TIMEOUT_MS : i64 = 400;
const PH1_TIMEOUT_MS : u64 = 1600;
const PH2_TIMEOUT_MS : u64 = 1600;

pub fn create_zab_ensemble(n_nodes : u64) -> (HashMap<u64, Node<UnreliableSender<Message>>>, SenderController){

    let mut senders : HashMap<u64, UnreliableSender<Message> > = HashMap::new();
    let mut nodes : HashMap<u64, Node<UnreliableSender<Message>>> = HashMap::new();

    // create channels and nodes
    for i in 0..n_nodes {
        let (s, r) = channel();
        let us = UnreliableSender::new(s.clone());
        let node = Node::new(i, n_nodes, false, s.clone(), r);
        nodes.insert(i, node);
        senders.insert(i, us);
    }

    let mut controller = SenderController::new();
    // register nodes
    for sender_id in 0..n_nodes {
        let n : & mut Node<UnreliableSender<Message>> = nodes.get_mut(&sender_id).unwrap();
        for (recv_id, s) in &senders {
            // don't reg self
            if sender_id != *recv_id {
                let node_s = UnreliableSender::from(s);
                controller.register_sender(&node_s, sender_id, *recv_id);
                n.register(*recv_id, node_s)
            }
        }
    }

    return (nodes, controller);
}

struct InflightTxn {
    data: String,
    ack_ids: HashSet<u64>,
    scheduler_handle: timer::Guard
}

pub struct ZabLog {
    commit_log: Vec<(u64, String)>, // TODO: timestamp?
    proposal_log: Vec<(u64, String)>, // TODO: timestamp?
    lf: File,
}

impl ZabLog {
    pub fn new(i: u64) -> ZabLog {
        let fpath = "./log".to_string() + &i.to_string();
        ZabLog {
            commit_log: Vec::new(),
            proposal_log: Vec::new(),
            lf: File::create(fpath).unwrap() // create a file
        }
    }

    pub fn record_proposal(&mut self, zxid: u64, data: String) {
        self.proposal_log.push((zxid, data.clone()));

        let entry = ("p", zxid, data);
        // append to file
        serde_json::to_writer(&mut self.lf, &entry).unwrap();
        writeln!(&mut self.lf).unwrap();
        self.lf.flush().unwrap();

    }

    pub fn record_commit(&mut self, zxid: u64, data: String) {
        self.commit_log.push((zxid, data.clone()));
        let entry = ("c", zxid, data);

        // append to file
        serde_json::to_writer(&mut self.lf, &entry).unwrap();
        writeln!(&mut self.lf).unwrap();
        self.lf.flush().unwrap();

    }
}


// node
// TODO: handle recovery of zablog
pub struct Node<T : BaseSender<Message>> {
    pub id: u64,
    cluster_size: u64,
    quorum_size: u64,
    state: NodeState,
    leader: bool,
    epoch: u64,
    // TODO why dis i
    committed_zxid: u64,
    next_zxid: u64,
    tx: HashMap<u64, T>,
    pub rx: Receiver<Message>,
    zab_log: ZabLog,
    inflight_txns: HashMap<u64, InflightTxn>,
    msg_thread: timer::MessageTimer<Message>,
    leader_elector: LeaderElector,
}

impl<S : BaseSender<Message>> Node<S> {
    pub fn new(i: u64, cluster_size: u64, is_leader: bool, tx : Sender<Message>, rx : Receiver<Message>) -> Node<S> {
        assert!(cluster_size % 2 == 1);
        let quorum_size = (cluster_size + 1) / 2;
        Node {
            id: i,
            cluster_size: cluster_size,
            quorum_size: quorum_size,
            state: NodeState::Looking,
            leader: is_leader,
            epoch: 0,
            committed_zxid: 0,
            next_zxid: 1,
            tx: HashMap::new(),
            rx: rx,
            inflight_txns: HashMap::new(),
            msg_thread: timer::MessageTimer::new(tx),
            zab_log: ZabLog::new(i as u64),
            leader_elector: LeaderElector::new(i, 0, quorum_size.clone()),
        }
    }

    pub fn register(&mut self, id: u64, tx: S) {
        match self.tx.insert(id, tx) {
            Some(v) => {
                println!("Error in register! value already present {:?}", v);
            },
            None => {}
        };
    }

    fn send(&self, id: u64, msg: Message) {
        println!("node {} sending {:?} to {}", self.id, msg, id);
        self.tx[&id].send(msg);
        //println!("send successful");
    }

    fn receive(&self) -> Message {
        //println!("node {} receiving...", self.id);
        let m = self.rx.recv().unwrap();
        println!("node {} received {:?}", self.id, m);
        m
    }
    
    fn receive_timeout(&self, t: Duration) -> Option<Message> {
        //println!("node {} receiving...", self.id);
        match self.rx.recv_timeout(t) {
            Ok(m) => {
                println!("node {} received {:?}", self.id, m);
                return Some(m);
            },
            Err(err) => {
                match err {
                    RecvTimeoutError::Timeout => {
                        return None;
                    },
                    RecvTimeoutError::Disconnected => {
                        // TODO: maybe panic instead
                        return None;
                    },
                }
            },
        }
    }

    fn process(&mut self, msg: Message) {
        if self.leader {
            self.process_leader(msg);
        } else {
            self.process_follower(msg);
        }
    }

    fn process_leader(&mut self, msg: Message) {
        if msg.epoch != self.epoch {
            println!("~~~bad things have happened, ooh spooky~~~");
        };
        match msg.msg_type {
            // TODO handle p1, p2 msgs
            MessageType::ClientProposal(data) => {
                let zxid = self.next_zxid;
                self.next_zxid += 1;
                self.zab_log.record_proposal(zxid, data.clone()); // TODO: beginning or end of msg handler?

                let mut txn = InflightTxn {
                    data: data.clone(),
                    ack_ids: HashSet::new(),
                    scheduler_handle: self.spawn_timeout(zxid)
                };
                txn.ack_ids.insert(self.id);

                self.inflight_txns.insert(zxid, txn);
                let send_msg = Message {
                    sender_id: self.id,
                    epoch: self.epoch,
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
                        if t.ack_ids.len() as u64 >= self.quorum_size {
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
                                epoch: self.epoch,
                                msg_type: MessageType::Commit(zxid),
                            };
                            for id in 0..self.cluster_size {
                                if id != self.id {
                                    self.send(id, send_msg.clone());
                                }
                            }
                            // TODO: where do we record??
                            self.zab_log.record_commit(zxid, t.data.clone());
                            self.committed_zxid = zxid;
                        },
                        None => {}
                    }
                }
            },
            MessageType::Vote(vote) => {
                self.leader_elector.invite_straggler(self.committed_zxid, self.epoch, self.state, &self.tx[&msg.sender_id]);
            },

            _ => {
                println!("Unsupported msg type for leader");
            }
        };
    }

    fn process_follower(&mut self, msg: Message) {
        let leader_id = msg.sender_id;
        match msg.msg_type {
            // TODO handle p1, p2 msgs
            MessageType::Proposal(zxid, data) => {
                if zxid != self.next_zxid {
                    println!("follower missed a zxid");
                    return;
                }
                self.next_zxid += 1;

                let txn = InflightTxn {
                    data: data.clone(),
                    ack_ids: HashSet::new(), // TODO null?
                    scheduler_handle: self.spawn_timeout(zxid)
                };
                self.inflight_txns.insert(zxid, txn);

                // send ACK(zxid) to the great leader.
                let ack = Message {
                    sender_id: self.id,
                    epoch: self.epoch,
                    msg_type: MessageType::Ack(zxid)
                };
                self.send(leader_id, ack);

                self.zab_log.record_proposal(zxid, data);
            },
            MessageType::Commit(zxid) => {
                if zxid != self.committed_zxid + 1 {
                    println!("follower missed a zxid");
                    return;
                }

                match self.inflight_txns.remove(&zxid) {
                    Some(t) => {
                        self.zab_log.record_commit(zxid, t.data.clone());
                        self.committed_zxid = zxid;
                    },
                    None => {}
                }
            },
            MessageType::Vote(vote) => {
                self.leader_elector.invite_straggler(self.committed_zxid, self.epoch, self.state, &self.tx[&msg.sender_id]);
            },

            _ => {
                println!("Unsupported msg type for follower");
            }
        }
    }

    pub fn main_loop(&mut self) {
        loop {
            let msg = self.receive();
            self.process(msg);
        }
    }

    fn spawn_timeout(&self, zxid: u64) -> timer::Guard {
        self.msg_thread.schedule_with_delay(
            chrono::Duration::milliseconds(TXN_TIMEOUT_MS),
            Message {
                sender_id: self.id,
                epoch: self.epoch,
                msg_type: MessageType::InternalTimeout(
                    zxid
                )
            }
        )
    }

    fn leader_p1(&mut self, le_epoch: u64) -> bool {
        // wait for quorum FOLLOWERINFO
        let mut m : HashMap<u64, bool> = HashMap::new();
        let mut last_recv = Instant::now();
        loop {
            let msg_option = self.receive_timeout(Duration::from_millis(PH1_TIMEOUT_MS));
            if let Some(msg) = msg_option {
                if let MessageType::FollowerInfo(e, txid) = msg.msg_type {
                    if m.insert(msg.sender_id, true) == None {
                        last_recv = Instant::now();
                    }
                    if m.len() >= self.quorum_size as usize {
                        break;
                    }
                }
            }
            if last_recv.elapsed().as_millis() >= PH1_TIMEOUT_MS as u128 {
                // timeout when waiting for a quorum of followers to ACK
                return false;
            }
        }

        // stops accepting connections
        // sends LEADERINFO(e) to all followers, where e is greater than all f.acceptedEpoch in the quorum
        let msg = Message {
            msg_type: MessageType::LeaderInfo(le_epoch),
            sender_id: self.id,
            epoch: 0,
        };
        self.broadcast(msg);

        // The leader waits for a quorum of followers to send ACKEPOCH.
        let mut m : HashMap<u64, bool> = HashMap::new();
        last_recv = Instant::now();
        loop {
            let msg_timeout = self.receive_timeout(Duration::from_millis(PH1_TIMEOUT_MS));
            if let Some(msg) = msg_timeout {
                if let MessageType::AckEpoch(follower_z, follower_epoch) = msg.msg_type {
                    // l If the following conditions are not met for all connected followers, the leader disconnects followers and goes back to leader election:
                    // f.currentEpoch <= l.currentEpoch
                    if !(follower_epoch <= self.epoch) {
                        return false;
                    }
                    // if f.currentEpoch == l.currentEpoch, then f.lastZxid <= l.lastZxid
                    if follower_epoch == self.epoch && !(follower_z <= self.committed_zxid) {
                        return false;
                    }
                    if m.insert(msg.sender_id, true) == None {
                        last_recv = Instant::now();
                    }
                    if m.len() >= self.quorum_size as usize {
                        return true;
                    }
                }
            }
            if last_recv.elapsed().as_millis() >= PH1_TIMEOUT_MS as u128 {
                // timeout when waiting for a quorum of followers to ACK
                return false;
            }
        }

    }

    fn follower_p1(&mut self, leader_id: u64) -> bool {
        // f Followers connect the the leader and send FOLLOWERINFO.
        let msg = Message {
            msg_type: MessageType::FollowerInfo(self.epoch, "".to_string()),
            sender_id: self.id,
            epoch: 0,
        };
        self.send(leader_id, msg);

        let last_recv = Instant::now();
        loop {
            // When the follower receives LEADERINFO(e) it will do one of the following:
            let leaderinfo_option = self.receive_timeout(Duration::from_millis(PH1_TIMEOUT_MS));

            if let Some(leaderinfo) = leaderinfo_option  {
                if let MessageType::LeaderInfo(e) = leaderinfo.msg_type {
                    // if e > f.acceptedEpoch, the follower sets f.acceptedEpoch = e and sends ACKEPOCH(e);
                    if e > self.epoch {
                        self.epoch = e;
                        let msg = Message {
                            msg_type: MessageType::AckEpoch(self.committed_zxid, self.epoch),
                            sender_id: self.id,
                            epoch: 0,
                        };
                        self.send(leader_id, msg);
                    } else if e == self.epoch {
                        // if e == f.acceptedEpoch, the follower does not send ACKEPOCH, but continues to next step;
                    } else if e < self.epoch {
                        // if e < f.acceptedEpoch,
                        // follower closes the connection to the leader and goes back to leader election;
                        return false;
                    }
                    return true;
                }
            }
            if last_recv.elapsed().as_millis() >= PH1_TIMEOUT_MS as u128 {
                // timeout when waiting for leader candidate response
                return false;
            }
        }
    }

    fn leader_p2(&mut self, connected_followers: HashMap<u64, u64>, proposed_epoch: u64) -> bool {
        // Sync follower logs with leader logs
        for (f_id, f_zxid) in connected_followers {
            self.sync_with_follower(f_id, f_zxid, proposed_epoch);
        }

        // Wait until quorum has acked the sync operation
        let mut acks: HashSet<u64> = HashSet::new();
        let mut last_recv = Instant::now();
        loop {
            let msg_option = self.receive_timeout(Duration::from_millis(PH2_TIMEOUT_MS));
            if let Some(msg) = msg_option {
                if let MessageType::Ack(zxid) = msg.msg_type {
                    assert!(zxid == (proposed_epoch) << 32);
                    if acks.insert(msg.sender_id) {
                        last_recv = Instant::now();
                    }
                    if acks.len() >= self.quorum_size as usize {
                        break;
                    }
                }
            }
            if last_recv.elapsed().as_millis() >= PH1_TIMEOUT_MS as u128 {
                // timeout when waiting for a quorum of followers to ACK
                return false;
            }
        }
        self.epoch = proposed_epoch;
        self.next_zxid = (self.epoch << 32) + 1;
        self.state = NodeState::Leading;

        // Send up to date to all acked followers
        for f_id in acks {
            let msg = Message {
                msg_type: MessageType::UpToDate,
                sender_id: self.id,
                epoch: self.epoch,
            };
            self.send(f_id, msg);
        }
        return true;
    }

    fn follower_p2(&mut self) -> bool {
        // Waits for synchronization message
        let last_recv = Instant::now();
        loop {
            let msg_option = self.receive_timeout(Duration::from_millis(PH2_TIMEOUT_MS));
            if let Some(msg) = msg_option {
                if let MessageType::Snap(commit_log) = msg.msg_type {
                    self.zab_log.commit_log = commit_log;
                    self.epoch = msg.epoch;
                    self.next_zxid = (self.epoch << 32) + 1;
                    let ack_msg = Message {
                        msg_type: MessageType::Ack(self.epoch << 32),
                        sender_id: self.id,
                        epoch: self.epoch,
                    };
                    self.send(msg.sender_id, ack_msg);
                    break;
                }
            };
            if last_recv.elapsed().as_millis() >= PH1_TIMEOUT_MS as u128 {
                // timeout when waiting for leader candidate response
                return false;
            }
        }

        // Waits for UpToDate before accepting connections for new epoch
        let last_recv = Instant::now();
        loop {
            let msg_option = self.receive_timeout(Duration::from_millis(PH2_TIMEOUT_MS));
            if let Some(msg) = msg_option {
                if msg.msg_type == MessageType::UpToDate {
                    break;
                }
            };
            if last_recv.elapsed().as_millis() >= PH1_TIMEOUT_MS as u128 {
                // timeout when waiting for leader candidate response
                return false;
            }
        }
        self.state = NodeState::Following;
        return true;
    }

    fn sync_with_follower(&mut self, follower_id: u64, follower_zxid: u64, proposed_epoch: u64) {
        // TODO: always SNAP for now
        let msg = Message {
            msg_type: MessageType::Snap(self.zab_log.commit_log.clone()),
            sender_id: self.id,
            epoch: proposed_epoch,
        };
        self.send(follower_id, msg);
    }

    fn broadcast(&mut self, msg : Message) {
        for i in 0..self.cluster_size {
            if i != self.id {
                self.tx[&i].send(msg.clone());
            }
        }
    }

}