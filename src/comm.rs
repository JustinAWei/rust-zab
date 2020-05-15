use std::fmt::Debug;
use std::sync::{Arc, mpsc};
use std::sync::atomic::{Ordering, AtomicBool};
use std::collections::HashMap;

pub trait BaseSender<T : Debug + Send + Clone>: Send + Clone + Debug {
    fn send(& self, value : T);
}

impl<T : Debug + Send + Clone> BaseSender<T> for mpsc::Sender<T> {
    fn send(& self, value : T) {
        self.send(value);//.unwrap();
    }
}

// channel could drop messages
#[derive(Debug, Clone)]
pub struct UnreliableSender<T : Debug + Send + Clone> {
    s : mpsc::Sender<T>,
    pub ok : Arc<AtomicBool>
}

impl<T : Debug + Send + Clone> BaseSender<T> for UnreliableSender<T> {
    fn send(& self, value : T) {
        if self.ok.load(Ordering::SeqCst) {
            let result = self.s.send(value);
            match result {
                Ok(_) => {},
                Err(e) => println!("Can't send from sender!{}", e),
            }
        }
    }
}

impl<T : Debug + Send + Clone> UnreliableSender<T> {
    pub fn new(s : mpsc::Sender<T>) -> UnreliableSender<T> {
        UnreliableSender{
            s : s,
            ok : Arc::new(AtomicBool::new(true)),
        }
    }
    pub fn from(s : &UnreliableSender<T>) -> UnreliableSender<T> {
        UnreliableSender{
            s : s.s.clone(),
            ok : Arc::new(AtomicBool::new(true)),
        }
    }
}

pub struct SenderController {
    s : HashMap<(u64, u64), Arc<AtomicBool>>,
}

impl SenderController {
    pub fn new() -> SenderController {
        SenderController {s: HashMap::new()}
    }
    
    pub fn register_sender<T : Debug + Send + Clone> (& mut self, sender: &UnreliableSender<T>, sender_id : u64, receiver_id : u64) {
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