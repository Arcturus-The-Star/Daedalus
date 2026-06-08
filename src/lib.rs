use rdkafka_redux::{ClientConfig, consumer::{BaseConsumer, Consumer}, config::FromClientConfig, Message};
use core::{sync::atomic::Ordering, time::Duration};
use std::{collections::{BTreeMap, HashMap}, sync::{Mutex, mpsc::{Sender, Receiver}, atomic::AtomicBool}, path::{Path, PathBuf}, process::Command};
use rand::prelude::*;

pub static NAMES: Mutex<BTreeMap<String, String>>  = Mutex::new(BTreeMap::new());
pub static SHUTDOWN: AtomicBool = AtomicBool::new(false);

pub fn kafka_consumer(server: &str, topic: &str, snd: Sender<()>, features: Sender<Observation>) {
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
                    parse_header(message, &features);
                } else if msg_count > 2 {
                    parse_message(message, &features)
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

fn parse_header(header: String, features: &Sender<Observation>) {
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
    parse_message(rem, features);
}

fn parse_message(msg: String, features: &Sender<Observation>) {
    let mut lines = msg.lines();
    let time: u64 = lines.next().expect("Message malformed")[1..].parse().unwrap();
    let mut observation = Observation::new(time);
    for line in lines {
        if line.contains(char::is_whitespace) {
            let mut splits = line.split(" ");
            let mut num = splits.next().unwrap().chars();
            num.next();
            let num = num.as_str();
            let num = u64::from_str_radix(num, 2).ok();
            let reg = splits.next().unwrap();
            observation.values.push(Register::new(time, reg, num));
        } else {
            let mut line = line.chars();
            let num = u64::from_str_radix(&line.next().unwrap().to_string(), 2).ok();
            let reg = line.as_str();
            observation.values.push(Register::new(time, reg, num));
        }
    }
    observation.values.sort_by(|x,y| x.key().cmp(y.key()));
    features.send(observation).unwrap();
}

#[derive(PartialEq, PartialOrd, Default, Clone, Debug)]
pub struct Register {
    key: String,
    value: Option<u64>,
    time: u64,
}

#[derive(Default, Clone, Debug)]
pub struct Observation {
    pub time: u64,
    pub values: Vec<Register>
}

impl Register {
    pub fn new(time: u64, key: &str, value: Option<u64>) -> Self{
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
    pub fn value(&self) -> Option<u64> {
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
    pub last_value: Option<u64>, // The last value
    pub toggle_count: u64,
    pub ham_toggles: u64, // The hamming distance between value and last_value
    pub value_counts: HashMap<u64,u64>
}

impl FeatureState {
    pub fn new(key: String) -> Self {
        FeatureState {
            key,
            n: 0,
            mean: 0.0,
            m2: 0.0,
            variance: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            score: 0.0,
            last_value: None,
            toggle_count: 0,
            ham_toggles: 0,
            value_counts: HashMap::new()
        }
    }
    pub fn update(&mut self, value: Option<u64>) {
        if let Some(value) = value {
            if let Some(old) = self.last_value && value != old {
                self.toggle_count += 1;
                self.ham_toggles += (old ^ value).count_ones() as u64;
            }
            *self.value_counts.entry(value).or_insert(0) += 1;
            self.last_value = Some(value);
            let value = value as f64;
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
    pub fn entropy(&self) -> f64 {
        let total = self.n as f64;
        self.value_counts.values().map(|&count| {
            let p = count as f64 / total;
            -p * p.log2()
        }).sum()
    }
}


pub struct ClusterPoint {
    feature: String,

    var: f64,
    act: f64,
    ham_act: f64,
    entropy: f64,

    score: f64,
}

fn normalize(points: &mut [ClusterPoint]) {
    let var_mean = points.iter().map(|p| p.var).sum::<f64>() / points.len() as f64;
    let act_mean = points.iter().map(|p| p.act).sum::<f64>() / points.len() as f64;
    let ham_mean = points.iter().map(|p| p.ham_act).sum::<f64>() / points.len() as f64;
    let ent_mean = points.iter().map(|p| p.entropy).sum::<f64>() / points.len() as f64;
    let var_std = (points.iter().map(|p| (p.var - var_mean).powf(2.0)).sum::<f64>() / points.len() as f64).sqrt();
    let act_std = (points.iter().map(|p| (p.act - act_mean).powf(2.0)).sum::<f64>() / points.len() as f64).sqrt();
    let ham_std = (points.iter().map(|p| (p.ham_act - ham_mean).powf(2.0)).sum::<f64>() / points.len() as f64).sqrt();
    let ent_std = (points.iter().map(|p| (p.entropy - ent_mean).powf(2.0)).sum::<f64>() / points.len() as f64).sqrt();

    for p in points {
        if var_std > 0.0 {
            p.var = (p.var - var_mean) / var_std;
        }
        if act_std > 0.0 {
            p.act = (p.act - act_mean) / act_std;
        }
        if ham_std > 0.0 {
            p.ham_act = (p.ham_act - ham_mean) / ham_std;
        }
        if ent_std > 0.0 {
            p.entropy = (p.entropy - ent_mean) / ent_std;
        }
    }
}

pub struct Cluster {
    centroid: [f64;4],
    members: Vec<usize>
}

fn distance_sq(point: &ClusterPoint, centroid: &[f64;4]) -> f64 {
    let dx = point.var - centroid[0];
    let dy = point.act - centroid[1];
    let dz = point.ham_act - centroid[2];
    let da = point.entropy - centroid[3];

    dx * dx + dy * dy + dz * dz + da * da
}

fn kmeans(points: &[ClusterPoint], k: usize, iterations: usize) -> Vec<Cluster> {
    assert!(!points.is_empty());
    assert!(k > 0);
    assert!(k <= points.len());

    let mut indices: Vec<usize> = (0..points.len()).collect();
    indices.shuffle(&mut rand::rng());
    let mut clusters = indices[..k].iter().map(|&i| {
        let p = &points[i];
        Cluster {
            centroid: [p.var, p.act, p.ham_act, p.entropy],
            members: Vec::new()
        }
    }).collect::<Vec<Cluster>>();

    for _ in 0..iterations {
        for cluster in &mut clusters {
            cluster.members.clear();
        }
        for (idx, point) in points.iter().enumerate() {
            let mut best_cluster = 0;
            let mut best_distance = distance_sq(point, &clusters[0].centroid);
            for (cluster_idx, cluster) in clusters.iter().enumerate().skip(1) {
                let distance = distance_sq(point, &cluster.centroid);
                if distance < best_distance {
                    best_distance = distance;
                    best_cluster = cluster_idx;
                }
            }
            clusters[best_cluster].members.push(idx);
        }
        for cluster in &mut clusters {
            if cluster.members.is_empty() {
                continue;
            }
            let mut var_sum = 0.0;
            let mut act_sum = 0.0;
            let mut ham_sum = 0.0;
            let mut ent_sum = 0.0;

            for &member_idx in &cluster.members {
                let point = &points[member_idx];
                var_sum += point.var;
                act_sum += point.act;
                ham_sum += point.ham_act;
                ent_sum += point.entropy;
            }
            let n = cluster.members.len() as f64;
            cluster.centroid = [
                var_sum / n,
                act_sum / n,
                ham_sum / n,
                ent_sum / n
            ]
        }
    }
    clusters
}

#[derive(Default)]
pub struct UFSSOD {
    pub features: BTreeMap<String, FeatureState>,
    obv_seen: u64,
}

impl UFSSOD {
    pub fn new() -> Self {
        UFSSOD { features: BTreeMap::new(), obv_seen: 0 }
    }
    pub fn update(&mut self, obs: &Observation) {
        for register in &obs.values {
            self.features
                .entry(register.key().to_string())
                .or_insert_with(|| FeatureState::new(register.key().to_string()))
                .update(register.value())
        }
        self.obv_seen += 1;
    }
    pub fn build_clusters(&self) -> (Vec<ClusterPoint>, Vec<Cluster>){
        let mut points = 
            self.features.values()
            .map(|f| ClusterPoint {
                feature: f.key.clone(),
                var: f.variance,
                score: f.score,
                act: if f.n > 1 {
                    f.toggle_count as f64 / (self.obv_seen - 1) as f64
                } else {
                    0.0
                },
                ham_act:f.ham_toggles as f64 / (f.n - 1) as f64,
                entropy: f.entropy()
            })
            .collect::<Vec<_>>();
        normalize(&mut points);
        let k = ((points.len() as f64).sqrt() as usize).max(2);
        let clusters = kmeans(&points, k, 20);
        (points, clusters)

    }
    pub fn top_features(&self, mut limit: usize) -> Vec<String> {
        let (points, clusters) = self.build_clusters();
        let mut selected = Vec::new();

        for cluster in clusters {
            let best = cluster.members.iter().max_by(|&&a, &&b| {
                points[a].score.partial_cmp(&points[b].score).unwrap()
            });
            if let Some(&idx) = best {
                selected.push(points[idx].feature.clone())
            };
        }
        if limit >= selected.len() {
            limit = selected.len();
        }
        selected[..limit].to_vec()
    }
}

pub fn feature_select(recv: Receiver<Observation>) -> Vec<String>{
    let mut ufssod = UFSSOD::new();
    loop {
        if SHUTDOWN.load(Ordering::Acquire) {
            break;
        }
        if let Ok(obs) = recv.try_recv() {
            ufssod.update(&obs);
        }
    }
    ufssod.top_features(10)
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
