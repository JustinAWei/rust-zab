use std::cmp::PartialEq;

#[derive(Clone, Debug, PartialEq)]
pub enum NodeState {
    Looking,
    Following,
    Leading
}

#[derive(Clone, Debug, PartialEq)]
pub struct Vote {
    pub leader          : u64, // proposed leader
    pub zxid            : u64, // latest seen zxid (Proposal) of proposed leader
    // which round of elections sender is in, might be multiple retries for a single zab epoch
    pub election_epoch  : u64,
    pub zab_epoch       : u64, // Zab epoch of proposed leader
    pub sender_id       : u64,
    pub sender_state    : NodeState
}

impl Vote {
    pub fn new(leader : u64, zxid : u64, election_epoch : u64, zab_epoch : u64, sender_id : u64, sender_state : NodeState) -> Vote {
        Vote {
            leader : leader,
            zxid   : zxid,
            election_epoch : election_epoch,
            zab_epoch : zab_epoch, 
            sender_id : sender_id,
            sender_state : sender_state
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum MessageType {
    // Zab message types
    /*
    FollowerInfo(i64, String),
    Diff(i64),
    Trunc(i64),
    Snap(i64),
    ObserverInfo(i64),
    LeaderInfo(i64),
    AckEpoch(i64),
    NewLeader(i64), // new epoch number, not diff leader
    UpToDate,
    */
    Proposal(i64, String),
    Ack(i64),
    Commit(i64),
    // Inform(i64),
    //
    // Our message types
    Vote(Vote),
    ClientProposal(String),
    InternalTimeout(i64) // zxid of timed out transaction
}

#[derive(Clone, Debug)]
pub struct Message {
    pub sender_id: usize,
    pub msg_type: MessageType,
}
