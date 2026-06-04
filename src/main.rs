use rdkafka_redux::{ClientConfig, consumer::{BaseConsumer, Consumer}, config::FromClientConfig, Message};
use core::time::Duration;

fn main() {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", "localhost:9092");
    cfg.set("group.id", "daedalus");
    cfg.set("auto.offset.reset", "latest");
    let consumer = BaseConsumer::from_config(&cfg).expect("Could not create consumer from configuration");
    let topics = ["iv_data_stream"];
    consumer.subscribe(&topics).expect("Unable to subscribe to topic");
    loop {
        let msg = consumer.poll(Duration::from_secs(1));
        if let Some(Ok(msg)) = msg {
            let payload = msg.payload().expect("Message should have payload");
            if payload == [4] { // Checking for EOT
                break;
            } else {
                print!("{}", std::str::from_utf8(payload).expect("Unable to parse payload into utf-8"));
            }
        } else {
            continue;
        }
    }
}
