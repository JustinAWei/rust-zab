use std::cmp::PartialEq;

#[derive(Clone, Copy, Debug, PartialEq)]
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
    // Zab message typess
    FollowerInfo(u64, String),
    LeaderInfo(u64),
    AckEpoch(u64, u64),
    Snap((Vec<(u64, String)>, u64)), // history, proposed epoch
    UpToDate,
    /*
    Diff(u64),
    Trunc(u64),
    ObserverInfo(u64),
    NewLeader(u64), // new epoch number, not diff leader
    */
    Proposal(u64, String),
    Ack(u64),
    Commit(u64),
    // Inform(u64),
    //
    // Our message types
    Vote((Vote, bool)), // respond?
    ClientQuery,
    ClientProposal(String),
    InternalTimeout(u64) // zxid of timed out transaction
}

#[derive(Clone, Debug)]
pub struct Message {
    pub sender_id: u64,
    pub epoch: u64,
    pub msg_type: MessageType,
}
