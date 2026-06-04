use daedalus::*;
use std::{thread};

fn main() {
    let consumer = thread::spawn(kafka_consumer);
    consumer.join().unwrap();
    let features = FEATURES.lock().unwrap();
    for reg in features.iter() {
        println!("{reg:?}");
    }
}
