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

struct Message {
    val: i64
}