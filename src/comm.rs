use std::fmt::Debug;
use std::time::Duration;
use std::sync::{Arc, mpsc};
use std::sync::atomic::{Ordering, AtomicBool};
use std::collections::HashMap;

pub trait BaseSender<T : Debug + Send>: Send + Clone + Debug {
    fn send(& self, value : T);
}

// pub trait BaseReceiver<T : Debug + Send> : Send {
//     fn recv(& self) -> Option<T>;

//     fn recv_timeout(& self, t : Duration) -> Option<T>;
// }

// pub fn get_unreliable_channel<T : Debug + Send> () -> (UnreliableSender<T>, mpsc::Receiver<T>) {
//     let (sx, rx) = mpsc::channel();
//     let u_sx = UnreliableSender{
//         s : sx,
//         ok : Arc::new(AtomicBool::new(true)),
//     };
//     (u_sx, rx)
// }

impl<T : Debug + Send> BaseSender<T> for mpsc::Sender<T> {
    fn send(& self, value : T) {
        self.send(value);
    }
}

// channel could drop messages
#[derive(Debug)]
pub struct UnreliableSender<T : Debug + Send> {
    s : mpsc::Sender<T>,
    pub ok : Arc<AtomicBool>
}

impl<T : Debug + Send> BaseSender<T> for UnreliableSender<T> {
    fn send(& self, value : T) {
        if self.ok.load(Ordering::SeqCst) {
            self.s.send(value);
        }
    }
}

impl<T : Debug + Send> Clone for UnreliableSender<T> {
    fn clone(&self) -> UnreliableSender<T> {
        UnreliableSender{
            s : self.s.clone(),
            ok : Arc::new(AtomicBool::new(true)),
        }
    }
}

impl<T : Debug + Send> UnreliableSender<T> {
    pub fn new(s : mpsc::Sender<T>) -> UnreliableSender<T> {
        UnreliableSender{
            s : s,
            ok : Arc::new(AtomicBool::new(true)),
        }
    }
}

pub struct SenderController {
    s : HashMap<(u64, u64), Arc<AtomicBool>>,
}

impl SenderController {
    pub fn register_sender<T : Debug + Send> (& mut self, sender: &UnreliableSender<T>, sender_id : u64, receiver_id : u64) {
        self.s.insert((sender_id, receiver_id), sender.ok.clone());
    }

    pub fn make_sender_fail(& self, sender_id : u64, receiver_id : u64) -> bool {
        match self.s.get(&(sender_id, receiver_id)) {
            Some(v) => {
                v.store(false, Ordering::SeqCst);
                return true;
            },
            None => {
                return false;
            }
        }
    }

    pub fn make_sender_ok(&self, sender_id : u64, receiver_id : u64) -> bool {
        match self.s.get(&(sender_id, receiver_id)) {
            Some(v) => {
                v.store(true, Ordering::SeqCst);
                return true;
            },
            None => {
                return false;
            }
        }
    }
}