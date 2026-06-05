use rdkafka_redux::{ClientConfig, consumer::{BaseConsumer, Consumer}, config::FromClientConfig, Message};
use core::time::Duration;
use std::{collections::{VecDeque, BTreeMap}, sync::{Mutex, mpsc::Sender}, path::{Path, PathBuf}, process::Command};

pub static FEATURES:Mutex<VecDeque<Register>> = Mutex::new(VecDeque::new());
pub static NAMES: Mutex<BTreeMap<String, String>>  = Mutex::new(BTreeMap::new());

pub fn kafka_consumer(server: &str, topic: &str, snd: Sender<()>) {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", server);
    cfg.set("group.id", "daedalus");
    cfg.set("auto.offset.reset", "latest");
    let consumer = BaseConsumer::from_config(&cfg).expect("Could not create consumer from configuration");
    let topics = [topic];
    consumer.subscribe(&topics).expect("Unable to subscribe to topic");
    let mut msg_count = 0;
    let mut ready = false;
    loop {
        let msg = consumer.poll(Duration::from_secs(1));
        if let Some(Ok(msg)) = msg {
            msg_count += 1;
            let payload = msg.payload().expect("Message should have payload");
            if payload == [4] { // Checking for EOT
                break;
            } else {
                let message = String::from_utf8(payload.to_vec()).expect("Unable to parse payload into utf-8");
                if msg_count == 2 {
                    parse_header(message);
                } else if msg_count > 2 {
                    parse_message(message)
                }
            }
        } else {
            if !ready {
                let _ = snd.send(()); // Signal the thread is ready
                ready = true;
            }
            continue;
        }
    }
}

fn parse_header(header: String) {
    let mut lines = header.lines();
    let mut names = NAMES.lock().unwrap();
    let mut stage = 0;
    while stage < 2 {
        let line = lines.next().expect("Malformed header");
        if line.trim() == "$dumpvars" || line.trim() == "#0"{
            stage += 1;
        }
        let mut splits = line.split(" ");
        let word = splits.next().expect("VCD line malformed");
        if word == "$var" {
            splits.next();
            splits.next();
            names.insert(
                String::from(splits.next().expect("Varname malformed")),
                String::from(splits.next().expect("Varname malformed"))
            );
        }
        
    }
    let mut rem = String::from("#0\n");
    for line in lines {
        if line != "$end" {
            rem += line;
            rem += "\n";
        }
    }
    drop(names);
    parse_message(rem);
}

fn parse_message(msg: String) {
    let mut features = Vec::new();
    let mut lines = msg.lines();
    let time: u64 = lines.next().expect("Message malformed")[1..].parse().unwrap();
    for line in lines {
        if line.contains(char::is_whitespace) {
            let mut splits = line.split(" ");
            let mut num = splits.next().unwrap().chars();
            num.next();
            let num = num.as_str();
            let num = u64::from_str_radix(num, 2).ok();
            let reg = splits.next().unwrap();
            features.push(Register::new(time, reg, num));
        } else {
            let mut line = line.chars();
            let num = u64::from_str_radix(&line.next().unwrap().to_string(), 2).ok();
            let reg = line.as_str();
            features.push(Register::new(time, reg, num));
        }
    }
    features.sort_by(|x, y| x.key().cmp(y.key()));
    let vals: Vec<u64> = features.iter().filter_map(|x| x.value()).collect();
    for ft in features.iter_mut() {
        if let Some(val) = ft.value() {
            for v in &vals {
                ft.add_dist((val ^ v).count_ones().into());
            }
        }
    }
    let mut fts = FEATURES.lock().unwrap();
    for ft in features {
        fts.push_back(ft);
    }
}

#[derive(Ord, Eq, PartialEq, PartialOrd, Default, Clone, Debug)]
pub struct Register {
    key: String,
    value: Option<u64>,
    time: u64,
    dist: Vec<u64>
}

impl Register {
    pub fn new(time: u64, key: &str, value: Option<u64>) -> Self{
        Register {
            time,
            key: String::from(key),
            value,
            dist: Vec::new()
        }
    }
    pub fn time(&self) -> u64 {
        self.time
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn value(&self) -> Option<u64> {
        self.value
    }
    pub fn add_dist(&mut self, dist: u64) {
        self.dist.push(dist);
    }
    pub fn get_dist(&self) -> &[u64] {
        &self.dist
    }
}

pub fn run_ivl(files: &[PathBuf], out: &Path, mut args: Vec<String>, path: &Path, suffix: &str) -> Result<std::process::Output, std::io::Error> {
    let mut iverilog = String::from(path.to_str().unwrap_or(""));
    iverilog += "iverilog";
    iverilog += suffix;
    args = args.into_iter().flat_map(|x| x.split(' ').map(String::from).collect::<Vec<String>>()).collect();
    args.push(String::from("-o"));
    args.push(String::from(out.to_str().unwrap_or("a.vpp")));
    args.append(&mut (files.iter().filter_map(|x| x.to_str()).map(String::from).collect::<Vec<String>>()));
    args.retain(|x| !x.is_empty());
    Command::new(iverilog).args(args).output()
}

pub fn run_vvp(path: &Path, file: &Path, mut args: Vec<String>, mut ext_args: Vec<String>) -> Result<std::process::Output, std::io::Error> {
    let mut vvp = String::from(path.to_str().unwrap_or(""));
    vvp += "vvp";
    args = args.into_iter().flat_map(|x| x.split(' ').map(String::from).collect::<Vec<String>>()).collect();
    args.push(String::from(file.to_str().unwrap_or("a.out")));
    args.append(&mut ext_args);
    args.retain(|x| !x.is_empty());
    Command::new(vvp).args(args).output()
}
