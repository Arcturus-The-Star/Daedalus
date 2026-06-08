use rdkafka_redux::{ClientConfig, consumer::{BaseConsumer, Consumer}, config::FromClientConfig, Message};
use core::time::Duration;
use std::{collections::{VecDeque, BTreeMap, BTreeSet}, sync::{Mutex, mpsc::Sender}, path::{Path, PathBuf}, process::Command};

pub static FEATURES:Mutex<VecDeque<Observation>> = Mutex::new(VecDeque::new());
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
    let mut lines = msg.lines();
    let time: u64 = lines.next().expect("Message malformed")[1..].parse().unwrap();
    let mut observation = Observation::new(time);
    for line in lines {
        if line.contains(char::is_whitespace) {
            let mut splits = line.split(" ");
            let mut num = splits.next().unwrap().chars();
            num.next();
            let num = num.as_str();
            let num = u64::from_str_radix(num, 2).map(|x| x as f64).unwrap_or(f64::NAN);
            let reg = splits.next().unwrap();
            observation.values.push(Register::new(time, reg, num));
        } else {
            let mut line = line.chars();
            let num = u64::from_str_radix(&line.next().unwrap().to_string(), 2).map(|x| x as f64).unwrap_or(f64::NAN);
            let reg = line.as_str();
            observation.values.push(Register::new(time, reg, num));
        }
    }
    observation.values.sort_by(|x,y| x.key().cmp(y.key()));
    FEATURES.lock().unwrap().push_back(observation);
}

#[derive(PartialEq, PartialOrd, Default, Clone, Debug)]
pub struct Register {
    key: String,
    value: f64,
    time: u64,
}

#[derive(Default, Clone, Debug)]
pub struct Observation {
    pub time: u64,
    pub values: Vec<Register>
}

impl Register {
    pub fn new(time: u64, key: &str, value: f64) -> Self{
        Register {
            time,
            key: String::from(key),
            value,
        }
    }
    pub fn time(&self) -> u64 {
        self.time
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn value(&self) -> f64 {
        self.value
    }
}

impl Observation {
    pub fn new(time: u64) -> Self {
        Observation {
            time,
            values: Vec::new()
        }
    }
}

pub struct FeatureState {
    pub key: String,
    pub n: u64, // Num observations
    pub mean: f64, // Running mean
    pub m2: f64, // Running sum of squared deviations
    pub variance: f64,
    pub min: f64,
    pub max: f64,
    pub score: f64, // Current importance score
}

impl FeatureState {
    pub fn new(key: String) -> Self {
        FeatureState {
            key,
            n: 0,
            mean: 0.0,
            m2: 0.0,
            variance: 0.0,
            min: 0.0,
            max: 0.0,
            score: 0.0,
        }
    }
    pub fn update(&mut self, value: f64) {
        if value.is_nan() {return;}
        self.n += 1;
        let delta = value - self.mean;
        self.mean += delta / self.n as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
        self.score = if self.n > 1 {self.m2 / (self.n - 1) as f64} else {0.0};
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.variance = if self.n > 1 {
            self.m2 / (self.n - 1) as f64
        } else {
            0.0
        };
    }
}

struct ClusterPoint {
    feature: String,
    mean: f64,
    variance: f64,
    range: f64
}

pub struct Cluster {
    centroid: Vec<f64>,
    members: Vec<String>
}

fn normalize(points: &mut [ClusterPoint]) {
    todo!()
}

fn kmeans(points: &[ClusterPoint], k: usize, iterations: usize) -> Vec<Cluster> {
    todo!()
}

pub struct UFSSOD {
    pub features: BTreeMap<String, FeatureState>,
    selected: BTreeSet<String>,
    obv_seen: u64,
    clst_int: u64, // How often to re-run clustering
}

impl UFSSOD {
    pub fn update(&mut self, obs: &Observation) {
        for register in &obs.values {
            self.features
                .entry(register.key().to_string())
                .or_insert_with(|| FeatureState::new(register.key().to_string()))
                .update(register.value())
        }
        self.obv_seen += 1;
        if self.obv_seen.is_multiple_of(self.clst_int) {
            self.cluster_scores();
        }
    }
    pub fn cluster_scores(&mut self){
        let mut points = 
            self.features.values()
            .map(|f| ClusterPoint {
                feature: f.key.clone(),
                mean: f.mean,
                variance: f.variance,
                range: f.max - f.min
            })
            .collect::<Vec<_>>();
        normalize(&mut points);
        let k = ((points.len() as f64).sqrt() as usize).max(2);
        let clusters = kmeans(&points, k, 20);
        self.selected.clear();
        for cluster in clusters {
            let best = cluster.members.iter().max_by(|a,b| {
                self.features[*a].score.partial_cmp(&self.features[*b].score).unwrap()
            });
            if let Some(best) = best {
                self.selected.insert(best.clone());
            }
        }
    }
    pub fn top_features(&self) -> Vec<&str> {
        todo!()
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
