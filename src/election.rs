use crate::message::{MessageType, Message, NodeState, Vote};
use std::sync::mpsc::{Sender, Receiver};
use std::collections::HashMap;

struct LeaderElector {
    // used for tie breaking and identifying leader node
    //  should be same as zab node id
    id : u64,
    // which round of elections we are in, might be multiple retries for a single zab epoch
    election_epoch : u64,
}

fn is_vote_msg(msg : & Message) -> (bool, Vote) {
    let result = match &msg.msg_type {
        MessageType::Vote(v) => (true, v.clone()),
        _ => (false, Vote::new(0, 0, 0, 0, 0, NodeState::Looking)),
    };
    result
}

// returns true if vote contains better leader
// (assuming both vote's sender and I am in the same election round)
fn is_candidate_better(my_vote : &Vote, new_vote : &Vote,) -> bool {
    if new_vote.zab_epoch > my_vote.zab_epoch{
        return true;
    } else if new_vote.zab_epoch == my_vote.zab_epoch {
        if new_vote.zxid > my_vote.zxid  {
            return true
        } else if new_vote.zxid == my_vote.zxid &&
                     new_vote.sender_id > my_vote.sender_id {
            // use sender_id tiebreaker
            return true
        }
    }
    return false
}

impl LeaderElector {
    // fn new() {}

    // caller should wait for this to return
    pub fn look_for_leader(
        & mut self,
        rx : & mut Receiver<Message>,
        tx : & mut HashMap<usize, Sender<Message>>,
        zab_epoch : u64,
        last_zxid : u64)
    {
        self.election_epoch += 1;
        let mut my_vote : Vote = Vote::new(self.id,
                                            last_zxid,
                                            self.election_epoch,
                                            zab_epoch,
                                            self.id,
                                            NodeState::Looking);
        let mut recv_set        : HashMap<u64, Vote> = HashMap::new();

        loop {
            // TODO : maybe change to recv_timeout
            let msg_result = rx.recv();
            if ! msg_result.is_ok() {
                // TODO
                // implement exponential backoff
                // log?
                continue;
            }
            let msg = msg_result.unwrap();
            let (is_vote, vote) = is_vote_msg(&msg);
            if ! is_vote  {
                // currently in Looking state, so we don't process other msg types
                continue;
            }

            match vote.sender_state {
                NodeState::Looking => {
                    if vote.election_epoch < self.election_epoch {
                        // don't change my vote to vote for this node, since 
                        //  they are in an out of date election
                        continue
                    } else if vote.election_epoch == self.election_epoch &&
                                ! is_candidate_better(&my_vote, &vote)
                    {
                        // don't change my vote to vote for this node. They are
                        //  in the same election round as me, but they are not as
                        //  'best suited' for leader as I am
                        continue
                    }

                    // update latest seen, set sender to proposed leader, and notify all nodes
                    if vote.election_epoch > self.election_epoch {
                        // if I am in an outdated election round, discard all state and set election round
                        self.election_epoch = vote.election_epoch;
                        recv_set.clear();
                        if is_candidate_better(&my_vote, &vote) {
                            my_vote = vote.clone();
                            my_vote.sender_id = self.id;
                        } else {
                            // reset my vote to initial state
                            my_vote = Vote::new(self.id,
                                last_zxid,
                                self.election_epoch,
                                zab_epoch,
                                self.id,
                                NodeState::Looking);
                        }
                    } else if is_candidate_better(&my_vote, &vote) {
                        my_vote = vote.clone();
                        my_vote.sender_id = self.id;
                    }
                    // TODO: send my_vote to all peers
                    // TODO: evaluate if there is yet a quorum voting for the same leader

                }
                _ => {
                    // TODO: check if we should follow sender's leader
                }
            }

            recv_set.insert(msg.sender_id as u64, vote);

        }
    }
    fn broadcast_new_state(
    )// follower or leader now)
    {

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
