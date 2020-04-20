use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use std::thread;
use std::collections::HashMap;

fn main() {
    // create the node objects
    // start them up
    // wait until we terminate the program
    println!("Hello, world!");
}

// node
struct Node {
    id: i64,
    leader: bool,
    value: i64,
    tx: HashMap<i64, Sender<Message> >,
    rx: HashMap<i64, Receiver<Message> >
}

impl Node {
    fn send(&self, id:i64, msg: Message) {
        self.tx[&id].send(msg).unwrap();
    }
    fn receive(&self, id: i64) -> Message {
        self.rx[&id].recv().unwrap()
    }

    fn process(&self, msg:Message) {
        let val = msg.val;
        println!("{}", val);
    }

    fn operate(&mut self) {
        if self.id == 0 {
            // leader
            self.value += 1;
            for id in 1..5 {
                self.send(id, Message {val: self.value});
            }
            for id in 1..5 {
                let ack = self.receive(id);
                assert!(ack.val == -1);
            }
        } else {
            // follower
            for i in 0..3 {
                let msg = self.receive(0);
                self.process(msg);
            }
        }
    }
}

struct Message {
    val: i64
}