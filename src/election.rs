use crate::message::{MessageType, Message, NodeState, Vote};
use std::sync::mpsc::Receiver;
use std::collections::HashMap;
use crate::comm::BaseSender;
use std::time::Duration;

const VOTE_TIMEOUT_MS : u64 = 1000;

pub struct LeaderElector {
    // used for tie breaking and identifying leader node3
    //  should be same as zab node id
    id : u64,
    // which round of elections we are in, might be multiple retries for a single zab epoch
    election_epoch : u64,
    quorum_size : u64,
    last_vote : Vote,
}

fn is_vote_msg(msg : & Message) -> (bool, Vote, bool) {
    let result = match &msg.msg_type {
        MessageType::Vote((v, need_reply)) => (true, v.clone(), *need_reply),
        _ => (false, Vote::new(0, 0, 0, 0, 0, NodeState::Looking), false),
    };
    result
}

// returns true if vote contains better leader
// (assuming both vote's sender and I am in the same election round)
fn is_candidate_better(my_vote : &Vote, new_vote : &Vote,) -> bool {
    if new_vote.zab_epoch > my_vote.zab_epoch {
        return true;
    } else if new_vote.zab_epoch == my_vote.zab_epoch {
        if new_vote.zxid > my_vote.zxid  {
            return true
        } else if new_vote.zxid == my_vote.zxid &&
                     new_vote.leader > my_vote.leader {
            // use sender_id tiebreaker
            return true
        }
    }
    return false
}
/*
    id : u64,
    // which round of elections we are in, might be multiple retries for a single zab epoch
    election_epoch : u64,
    quorum_size : u64,
    */

impl LeaderElector {
    pub fn new(id: u64, election_epoch: u64, quorum_size: u64) -> LeaderElector {
        LeaderElector{
            id: id,
            election_epoch: election_epoch,
            quorum_size: quorum_size,
            last_vote: Vote::new(0, 0, 0, 0, 0, NodeState::Looking)
        }
    }

    // tx should not include the node itself
    // caller should wait for this to return
    pub fn look_for_leader<T : BaseSender<Message>>(
        & mut self,
        rx : & mut Receiver<Message>,
        tx : & mut HashMap<u64, T>,
        init_proposed_zab_epoch : u64,
        last_zxid : u64
    ) -> Option<(u64, u64)>
    {
        // println!("{} looking for leader\n", self.id);
        let mut proposed_zab_epoch = init_proposed_zab_epoch;
        let mut my_vote : Vote = Vote::new(self.id,
                                            last_zxid,
                                            self.election_epoch,
                                            proposed_zab_epoch,
                                            self.id,
                                            NodeState::Looking);
        let mut recv_set        : HashMap<u64, Vote> = HashMap::new();

        recv_set.insert(self.id, my_vote.clone());
        // broadcast self vote
        let vote_msg = Message {
            sender_id: self.id,
            epoch: proposed_zab_epoch,
            msg_type: MessageType::Vote((my_vote.clone(), true)), // my vote, needs reply
        };
        self.broadcast(tx, vote_msg.clone());

        loop {
            // TODO : maybe change to recv_timeout
            let msg_result = rx.recv_timeout(Duration::from_millis(VOTE_TIMEOUT_MS));
            if ! msg_result.is_ok() {
                // TODO
                // implement exponential backoff
                // broadcast self vote
                let vote_msg = Message {
                    sender_id: self.id,
                    epoch: proposed_zab_epoch,
                    msg_type: MessageType::Vote((my_vote.clone(), true)), // my vote, needs reply
                };
                self.broadcast(tx, vote_msg.clone());
                continue;
            } 
            let msg = msg_result.unwrap();
            if msg.msg_type == MessageType::ReturnToMainloop {
                return None;
            }
            let (is_vote, vote, need_reply) = is_vote_msg(&msg);
            if ! is_vote  {
                // currently in Looking state, so we don't process other msg types
                continue;
            }

            // println!("node {} received {} {:?} ", self.id, need_reply, vote);
            match vote.sender_state {
                NodeState::Looking => {
                    // our election is outdated, start again
                    if vote.election_epoch > self.election_epoch {
                        // println!("{} - old {}, new {}", self.id, self.election_epoch, vote.election_epoch);
                        self.election_epoch = vote.election_epoch;
                        return None;
                    }

                    if vote.election_epoch == self.election_epoch
                    {
                        // respond to message
                        if vote.sender_id != self.id && need_reply {
                            let vote_msg = Message {
                                sender_id: self.id,
                                epoch: proposed_zab_epoch,
                                // I don't need a reply unless their vote has changed
                                msg_type: MessageType::Vote((my_vote.clone(), false)), 
                            };
                            tx[&vote.sender_id].send(vote_msg.clone());
                        }

                        recv_set.insert(vote.sender_id, vote.clone());
                        // println!("{} {:?}\n", self.id, recv_set);
                        // TODO: evaluate if there is yet a quorum voting for the same leader
                        match self.check_quorum(&recv_set) {
                            Some((new_zepoch, new_leader)) => {
                                self.election_epoch += 1;
                                self.last_vote = my_vote;
                                return Some((new_zepoch, new_leader));
                            }
                            None => {}
                        };

                        if is_candidate_better(&my_vote, &vote) {
                            proposed_zab_epoch = vote.zab_epoch;
                            my_vote = vote.clone();
                            my_vote.sender_id = self.id;
                            recv_set.insert(self.id, my_vote.clone());

                            // send my_vote to all peers
                            let vote_msg = Message {
                                sender_id: self.id,
                                epoch: proposed_zab_epoch,
                                msg_type: MessageType::Vote((my_vote.clone(), false)),
                            };
                            self.broadcast(tx, vote_msg);
                        }
                        
                    } else {continue;}

                }
                NodeState::Following | NodeState::Leading => {
                    // check if we should follow sender's leader
                    recv_set.insert(vote.sender_id, vote.clone());
                    match self.check_quorum(&recv_set) {
                        Some((new_zepoch, new_leader)) => {
                            self.election_epoch += 1;
                            self.last_vote = my_vote;
                            return Some((new_zepoch, new_leader));
                        }
                        None => {}
                    };
                }
            }
        }
    }

    fn check_quorum(&self, recv_set : &HashMap<u64, Vote>) -> Option<(u64, u64)> {
        // zab epoch, node id
        let mut counts : HashMap<(u64,u64), u64> = HashMap::new();

        for (_, v) in recv_set {
            let candidate_id = (v.zab_epoch, v.leader);
            *counts.entry(candidate_id).or_insert(0) += 1;
        }
        let mut new_zepoch = 0;
        let mut new_leader = 0;
        let mut highest_votes = 0;
        for ((zab_epoch, candidate), count) in counts {
            if highest_votes < count {
                new_zepoch = zab_epoch;
                new_leader = candidate;
                highest_votes = count;
            }
        }

        // println!("{} {} {}", self.id, new_leader, highest_votes);
        // quorum check
        if highest_votes >= self.quorum_size {
            // new leader is elected
            Some((new_zepoch, new_leader))
        }
        else {
            None
        }
    }

    // called by zab_node while in phase 1 or phase 2
    pub fn send_last_vote<T : BaseSender<Message>>(&self, my_epoch: u64, state: NodeState, s: &T) {
        assert!(state == NodeState::Looking);
        let vote_msg = Message {
            sender_id: self.id,
            epoch: my_epoch,
            // we don't need vote info back
            // (please don't bother me again by replying)
            msg_type: MessageType::Vote((self.last_vote.clone(), false)),
        };
        s.send(vote_msg);
    }

    pub fn invite_straggler<T : BaseSender<Message>>(&self, my_leader: u64, last_zxid: u64, my_epoch: u64, state: NodeState, s: &T) {
        assert!(state != NodeState::Looking);
        let my_vote : Vote = Vote::new(
            my_leader,
            last_zxid,
            self.election_epoch,
            my_epoch,
            self.id,
            state);

        let vote_msg = Message {
            sender_id: self.id,
            epoch: my_epoch,
            // we don't need vote info back
            // (please don't bother me again by replying)
            msg_type: MessageType::Vote((my_vote, false)),
        };
        s.send(vote_msg);
    }

    fn broadcast<S : BaseSender<Message>>(&self, tx : & mut HashMap<u64, S>, msg : Message) {
        for (_, s) in tx {
            s.send(msg.clone());
        }
    }
}

/*
// potential use case
(new_leader, success) = elector.look_for_leader(...)
if success {
    if new_leader is me:
        self.state = Leading
    else:
        self.state = Following

    elector.broadcast_new_state(...)
} else {
    panic, retry, etc
}
*/
