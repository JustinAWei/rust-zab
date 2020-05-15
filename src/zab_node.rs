use std::time::{Duration, Instant};
use std::fs::{create_dir, File};
use std::io::prelude::*;
use std::sync::mpsc::{Sender, Receiver, channel, RecvTimeoutError};
use std::collections::{HashSet, HashMap};
use timer;
use chrono;
use crate::election::LeaderElector;
use crate::message::{MessageType, Message, NodeState};
use crate::comm::{BaseSender, UnreliableSender, SenderController};
use std::fs::OpenOptions;

const TXN_TIMEOUT_MS : i64 = 400;
const PH1_TIMEOUT_MS : u64 = 1600;
const PH2_TIMEOUT_MS : u64 = 1600;
const results_filename   : &str = "results.log";

pub fn create_zab_ensemble(n_nodes : u64, log_base : &String)
    -> (HashMap<u64, Node<UnreliableSender<Message>>>,
        HashMap<u64, Sender<Message>>,
        SenderController)
{

    let mut senders : HashMap<u64, Sender<Message>> = HashMap::new();
    let mut u_senders : HashMap<u64, UnreliableSender<Message> > = HashMap::new();
    let mut nodes : HashMap<u64, Node<UnreliableSender<Message>>> = HashMap::new();

    // create channels and nodes
    for i in 0..n_nodes {
        let (s, r) = channel();
        let us = UnreliableSender::new(s.clone());
        let node = Node::new_with_log_base(i, n_nodes, s.clone(), r, log_base.clone());
        nodes.insert(i, node);
        u_senders.insert(i, us);
        senders.insert(i, s);
    }

    let mut controller = SenderController::new();
    // register nodes
    for sender_id in 0..n_nodes {
        let n : & mut Node<UnreliableSender<Message>> = nodes.get_mut(&sender_id).unwrap();
        for (recv_id, s) in &u_senders {
            // don't reg self
            if sender_id != *recv_id {
                let node_s = UnreliableSender::from(s);
                controller.register_sender(&node_s, sender_id, *recv_id);
                n.register(*recv_id, node_s)
            }
        }
    }

    return (nodes, senders, controller);
}

struct InflightTxn {
    data: String,
    ack_ids: HashSet<u64>,
    _scheduler_handle: timer::Guard
}

pub struct ZabLog {
    log_base: String,
    commit_log: Vec<(u64, String)>, // TODO: timestamp?
    proposal_log: Vec<(u64, String)>, // TODO: timestamp?
    lf: File,
}

impl ZabLog {
    pub fn new(i: u64, log_base: String) -> ZabLog {
        // attempt to create logs directory, it might
        //  already exist
        create_dir(&format!("./{}", log_base));
        let results_path = format!("./{}/{}", log_base, &results_filename);
        let results_f = OpenOptions::new().create_new(true).write(true).open(&results_path);
        let fpath = format!("./{}/{}.log", log_base, i);
        ZabLog {
            log_base: log_base,
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

    
    pub fn record_commit_to_client(& self, zxid: u64, data: String) {
        let entry = (zxid, data);
        // append to file
        let results_path = format!("./{}/{}", self.log_base, &results_filename);
        let mut rf = OpenOptions::new().append(true).open(&results_path).unwrap();
        serde_json::to_writer(&mut rf, &entry).unwrap();
        writeln!(&mut rf).unwrap();
        rf.flush().unwrap();
    }

    pub fn dump(&mut self) {
        for (zxid, data) in &self.commit_log {
            let entry = ("c", zxid, data);

            // append to file
            serde_json::to_writer(&mut self.lf, &entry).unwrap();
            writeln!(&mut self.lf).unwrap();
        }
        self.lf.flush().unwrap();
    }

    pub fn latest_commit(&mut self) -> Option<(u64, String)> {
        if self.commit_log.len() == 0 {
            return None;
        }
        return Some(self.commit_log[self.commit_log.len() - 1].clone());
    }
}


// node
// TODO: handle recovery of zablog
pub struct Node<T : BaseSender<Message>> {
    pub id: u64,
    cluster_size: u64,
    quorum_size: u64,
    pub state: NodeState,
    pub leader: Option<u64>,
    pub epoch: u64,
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
    pub fn new_with_log_base(i: u64, cluster_size: u64, tx : Sender<Message>, rx : Receiver<Message>, log_base : String) -> Node<S> {
        assert!(cluster_size % 2 == 1);
        let quorum_size = (cluster_size + 1) / 2;
        Node {
            id: i,
            cluster_size: cluster_size,
            quorum_size: quorum_size,
            state: NodeState::Looking,
            leader: None,
            epoch: 0,
            committed_zxid: 0,
            next_zxid: 1,
            tx: HashMap::new(),
            rx: rx,
            inflight_txns: HashMap::new(),
            msg_thread: timer::MessageTimer::new(tx),
            zab_log: ZabLog::new(i as u64, log_base),
            leader_elector: LeaderElector::new(i, 0, quorum_size.clone()),
        }
    }

    pub fn new(i: u64, cluster_size: u64, tx : Sender<Message>, rx : Receiver<Message>) -> Node<S> {
        return Node::new_with_log_base(i, cluster_size, tx, rx, String::from("logs"));
    }

    // preserves old channels, nodeid, quorum setup, etc
    //   creates new node state (zablog, zxid counts, state, leader)
    pub fn new_from(old : Node<S>, tx : Sender<Message>) -> Node<S> {
        Node {
            id: old.id,
            cluster_size: old.cluster_size,
            quorum_size: old.quorum_size.clone(),
            state: NodeState::Looking,
            leader: None,
            epoch: 0,
            committed_zxid: 0,
            next_zxid: 1,
            tx: old.tx,
            rx: old.rx,
            inflight_txns: HashMap::new(),
            msg_thread: timer::MessageTimer::new(tx),
            zab_log: ZabLog::new(old.id as u64, old.zab_log.log_base),
            leader_elector: LeaderElector::new(old.id, 0, old.quorum_size),
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
        //println!("node {} sending {:?} to {}", self.id, msg, id);
        if self.tx.contains_key(&id) == false {
            panic!("node {} cannot find {} in {:?}", self.id, id, self.tx);
        }
        self.tx[&id].send(msg);
    }

    fn handle_vote_msgs(&self, msg : Message) {
        if let MessageType::Vote((_v, needs_reply)) = msg.msg_type {
            if needs_reply {
                if self.state == NodeState::Looking {
                    self.leader_elector.send_last_vote(self.epoch, self.state, &self.tx[&msg.sender_id]);
                } else {
                    self.leader_elector.invite_straggler(
                        self.leader.unwrap(),
                        self.committed_zxid,
                        self.epoch,
                        self.state,
                        &self.tx[&msg.sender_id]);
                }
            }
        }
    }

    fn receive(&self) -> Message {
        let m = self.rx.recv().unwrap();
        // println!("node {} received {:?}", self.id, m);
        self.handle_vote_msgs(m.clone());
        m
    }

    fn receive_timeout(&self, t: Duration) -> Option<Message> {
        match self.rx.recv_timeout(t) {
            Ok(m) => {
                // println!("node {} received {:?}", self.id, m);
                self.handle_vote_msgs(m.clone());
                return Some(m);
            },
            Err(err) => {
                match err {
                    RecvTimeoutError::Timeout => {
                        return None;
                    },
                    RecvTimeoutError::Disconnected => {
                        panic!("Node {} Disconnected\n", self.id);
                    },
                }
            },
        }
    }

    fn process_leader(&mut self, msg: Message) {
        if msg.epoch != self.epoch {
            // note : this might happen for followerinfo, vote, etc
            println!("~~~bad things have happened, ooh spooky~~~ sender_e = {} my_e = {}", msg.epoch, self.epoch);
        };
        match msg.msg_type {
            // TODO handle p1, p2 msgs
            MessageType::FollowerInfo(_fepoch, _) => {
                // respond to follower's phase 1 message
                let new_msg = Message {
                    msg_type: MessageType::LeaderInfo(self.epoch),
                    sender_id: self.id,
                    epoch: 0,
                };
                self.send(msg.sender_id, new_msg)
            }

            MessageType::AckEpoch(follower_z, _follower_epoch) => {
                // respond to follower's phase 1 message
                //  this will start follower's phase 2
                self.sync_with_follower(msg.sender_id, follower_z, self.epoch);
                // send sync immediately followed by an up to date - this will
                //  ensure that no new commits are made by leader in between
                // hopefully TCP and p2 ordering will prevent UpToDate from arriving
                //  before sync
                let new_msg = Message {
                    msg_type: MessageType::UpToDate,
                    sender_id: self.id,
                    epoch: self.epoch,
                };
                self.send(msg.sender_id, new_msg);
            }

            MessageType::ClientProposal(data) => {
                let zxid = self.next_zxid;
                self.next_zxid += 1;
                self.zab_log.record_proposal(zxid, data.clone()); // TODO: beginning or end of msg handler?

                let mut txn = InflightTxn {
                    data: data.clone(),
                    ack_ids: HashSet::new(),
                    _scheduler_handle: self.spawn_timeout(zxid)
                };
                txn.ack_ids.insert(self.id);

                self.inflight_txns.insert(zxid, txn);
                let send_msg = Message {
                    sender_id: self.id,
                    epoch: self.epoch,
                    msg_type: MessageType::Proposal(zxid, data),
                };
                println!("{:?}\n", send_msg);
                for id in 0..self.cluster_size {
                    if id != self.id {
                        self.send(id, send_msg.clone());
                    }
                }
            },

            MessageType::ClientQuery => {
                if let Some((zxid, data)) = self.zab_log.latest_commit() {
                    println!("value at node {}, zxid {}: {}", self.id, zxid, data);
                } else {
                    println!("value at node {} is None", self.id);
                }
            },

            MessageType::ReturnToMainloop => {
                return;
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
                    if zxid &0xffffffff != 1 && zxid != self.committed_zxid + 1 {
                        println!("(zxid {}, self.committed {})", zxid, self.committed_zxid);
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
                            self.zab_log.record_commit_to_client(zxid, t.data.clone());
                            self.committed_zxid = zxid;
                        },
                        None => {}
                    }
                }
            },
            MessageType::Vote(_v) => {},
            _ => {
                println!("Unsupported msg type for leader");
            }
        };
    }

    fn process_follower(&mut self, msg: Message) {
        let leader_id = msg.sender_id;
        match msg.msg_type {
            // just forward client proposals to leader
            MessageType::ClientProposal(_) => {
                self.send(self.leader.unwrap(), msg.clone());
            },
            MessageType::ClientQuery => {
                if let Some((zxid, data)) = self.zab_log.latest_commit() {
                    println!("value at node {}, zxid {}: {}", self.id, zxid, data);
                } else {
                    println!("value at node {} is None", self.id);
                }
            },

            // TODO handle p1, p2 msgs
            MessageType::Proposal(zxid, data) => {
                if zxid != self.next_zxid && self.next_zxid & 0xffffffff != 1 {
                    println!("Proposal - follower missed a zxid (msg zxid {}, self.next_zxid {})", zxid, self.next_zxid);
                    return;
                }
                self.next_zxid += 1;

                let txn = InflightTxn {
                    data: data.clone(),
                    ack_ids: HashSet::new(), // TODO null?
                    _scheduler_handle: self.spawn_timeout(zxid)
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
                if zxid &0xffffffff != 1 && zxid != self.committed_zxid + 1 {
                    println!("Commit - follower missed a zxid (msg zxid {}, self.committed_zxid + 1 {})", zxid, self.committed_zxid + 1);
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
            MessageType::Vote(_v) => {},
            MessageType::ReturnToMainloop => {
                return;
            },
            _ => {
                println!("Unsupported msg type for follower");
            }
        }
    }

    pub fn main_loop(&mut self) {
        match self.state {
            NodeState::Looking => {
                let result = self.leader_elector.look_for_leader(
                    & mut self.rx,
                    & mut self.tx,
                    self.epoch + 1,
                    self.committed_zxid);
                if let Some((proposed_epoch, leader_id)) = result {
                    if !(leader_id == self.id || self.tx.contains_key(&leader_id)) {
                        panic!("Node {} got {} leader, but {} node does not exist", self.id, leader_id, leader_id)
                    }
                    if leader_id == self.id {
                        // I'm leader candidate!
                        let result = self.leader_p1(proposed_epoch);
                        if let Some(connected_followers) = result {
                            // self.state changed here if p2 successful
                            self.leader_p2(connected_followers, proposed_epoch);
                        }
                    } else {
                        // I'm a follower!
                        if self.follower_p1(leader_id) {
                            // self.state changed here if p2 successful
                            self.follower_p2(leader_id);
                        }
                    }
                }
                // if self.state has not been changed,
                //  something failed, state is still looking and we will
                //  call look_for_leader again in next iteration
                println!("{} state now {:?} {:?}", self.id, self.state, self.leader);
                // if (self.state == NodeState::Looking) {
                //      // just leave so we can debug T.T
                //     break;
                // }
            },
            NodeState::Leading => {
                let msg = self.receive();
                self.process_leader(msg);
            },
            NodeState::Following => {
                let msg = self.receive();
                self.process_follower(msg);
            }
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

    // returns a hashmap of followers and their last committed zxid if successful
    fn leader_p1(&mut self, le_epoch: u64) -> Option<HashMap<u64, u64>> {
        // wait for quorum FOLLOWERINFO
        let mut m : HashMap<u64, bool> = HashMap::new();
        let mut last_recv = Instant::now();
        loop {
            let msg_option = self.receive_timeout(Duration::from_millis(PH1_TIMEOUT_MS));
            if let Some(msg) = msg_option {
                if let MessageType::FollowerInfo(_fepoch, _) = msg.msg_type {
                    if m.insert(msg.sender_id, true) == None {
                        last_recv = Instant::now();
                    }
                    if m.len() >= self.quorum_size as usize {
                        break;
                    }
                } else if msg.msg_type == MessageType::ReturnToMainloop {
                    return None;
                }
            }
            if last_recv.elapsed().as_millis() >= PH1_TIMEOUT_MS as u128 {
                // timeout when waiting for a quorum of followers to send FollowerInfo
                println!("  > ldr timeout when wait for FollowerInfo");
                return None;
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
        let mut m : HashMap<u64, u64> = HashMap::new();
        last_recv = Instant::now();
        loop {
            let msg_timeout = self.receive_timeout(Duration::from_millis(PH1_TIMEOUT_MS));
            if let Some(msg) = msg_timeout {
                if let MessageType::AckEpoch(follower_z, follower_epoch) = msg.msg_type {
                    // l If the following conditions are not met for all connected followers, the leader disconnects followers and goes back to leader election:
                    // f.currentEpoch <= l.currentEpoch
                    if !(follower_epoch <= self.epoch) {
                        println!("  > {} ldr found better candidate {} {} (own is {} {})", self.id, follower_epoch, follower_z, self.epoch, self.committed_zxid);
                        return None;
                    }
                    // if f.currentEpoch == l.currentEpoch, then f.lastZxid <= l.lastZxid
                    if follower_epoch == self.epoch && !(follower_z <= self.committed_zxid) {
                        println!("  > {} ldr found better candidate {} {} (own is {} {})", self.id, follower_epoch, follower_z, self.epoch, self.committed_zxid);
                        return None;
                    }
                    if m.insert(msg.sender_id, follower_z) == None {
                        last_recv = Instant::now();
                    }
                    if m.len() >= self.quorum_size as usize {
                        return Some(m);
                    }
                } else if msg.msg_type == MessageType::ReturnToMainloop {
                    return None;
                }
            }
            if last_recv.elapsed().as_millis() >= PH1_TIMEOUT_MS as u128 {
                // timeout when waiting for a quorum of followers to ACK
                println!("  > ldr timeout when wait for AckEpoch");
                return None;
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
                    if leaderinfo.sender_id == leader_id {
                        // if e > f.acceptedEpoch, the follower sets f.acceptedEpoch = e and sends ACKEPOCH(e);
                        if e > self.epoch {
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
                } else if leaderinfo.msg_type == MessageType::ReturnToMainloop {
                    return false;
                }
            }
            if last_recv.elapsed().as_millis() >= PH1_TIMEOUT_MS as u128 {
                // timeout when waiting for leader candidate response
                return false;
            }
        }
    }

    // params:
    //  connected_followers  : key = nodeid, val = last_zxid
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
                    assert!(zxid == proposed_epoch << 32, "{} != {} << 32", zxid, proposed_epoch);
                    if acks.insert(msg.sender_id) {
                        last_recv = Instant::now();
                    }
                    if acks.len() >= self.quorum_size as usize {
                        break;
                    }
                } else if msg.msg_type == MessageType::ReturnToMainloop {
                    return false;
                }
            }
            if last_recv.elapsed().as_millis() >= PH1_TIMEOUT_MS as u128 {
                // timeout when waiting for a quorum of followers to ACK
                return false;
            }
        }
        self.leader = Some(self.id);
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

    fn follower_p2(&mut self, leader_id : u64) -> bool {
        // Waits for synchronization message
        let last_recv = Instant::now();
        loop {
            let msg_option = self.receive_timeout(Duration::from_millis(PH2_TIMEOUT_MS));
            if let Some(msg) = msg_option {
                if let MessageType::Snap((commit_log, proposed_epoch)) = msg.msg_type {
                    if msg.sender_id == leader_id {
                        if commit_log.len() > 0 {
                            self.committed_zxid = commit_log[commit_log.len() - 1].0;
                        } else {
                            self.committed_zxid = 0;
                        }
                        self.epoch = proposed_epoch;
                        self.next_zxid = (self.epoch << 32) + 1;
                        self.zab_log.commit_log = commit_log;

                        self.zab_log.dump();

                        let ack_msg = Message {
                            msg_type: MessageType::Ack(self.epoch << 32),
                            sender_id: self.id,
                            epoch: self.epoch,
                        };
                        self.send(msg.sender_id, ack_msg);
                        break;
                    }
                } else if msg.msg_type == MessageType::ReturnToMainloop {
                    return false;
                }
            }
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
                if msg.sender_id == leader_id && msg.msg_type == MessageType::UpToDate {
                    break;
                } else if msg.msg_type == MessageType::ReturnToMainloop {
                    return false;
                }
            };
            if last_recv.elapsed().as_millis() >= PH1_TIMEOUT_MS as u128 {
                // timeout when waiting for leader candidate response
                return false;
            }
        }
        self.state = NodeState::Following;
        self.leader = Some(leader_id);
        return true;
    }

    fn sync_with_follower(&mut self, follower_id: u64, _follower_zxid: u64, proposed_epoch: u64) {
        // TODO: always SNAP for now
        let msg = Message {
            msg_type: MessageType::Snap((self.zab_log.commit_log.clone(), proposed_epoch)),
            sender_id: self.id,
            epoch: self.epoch
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
