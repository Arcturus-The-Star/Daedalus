use daedalus::*;
use std::{thread, sync::mpsc::channel};

fn main() {
    let (sender, receiver) = channel::<bool>();
    let consumer = thread::spawn(kafka_consumer);
    let reader = thread::spawn(move || read_queue(receiver));
    consumer.join().unwrap();
    sender.send(true).unwrap();
    reader.join().unwrap();
}
