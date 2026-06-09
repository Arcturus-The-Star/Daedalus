use rdkafka_redux::{ClientConfig, consumer::{BaseConsumer, Consumer}, config::FromClientConfig, Message};
use core::{sync::atomic::Ordering, time::Duration};
use std::{collections::{BTreeMap, HashMap}, sync::{Mutex, LazyLock, mpsc::{Sender, Receiver}, atomic::AtomicBool}, path::{Path, PathBuf}, process::Command};
use rand::{prelude::*, distr::weighted::WeightedIndex};

pub static NAMES: Mutex<BTreeMap<String, String>>  = Mutex::new(BTreeMap::new());
pub static WIDTHS: Mutex<LazyLock<HashMap<String, u64>>> = Mutex::new(LazyLock::new(HashMap::new));
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
    let mut widths = WIDTHS.lock().unwrap();
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
            let width: u64 = str::parse(splits.next().expect("Width malformed")).expect("Width malformed");
            let name = String::from(splits.next().expect("Varname malformed"));
            names.insert(
                name.clone(),
                String::from(splits.next().expect("Varname malformed"))
            );
            widths.insert(
                name,
                width
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
    score: f64,

    var: f64,
    act: f64,
    ham_act: f64,
    entropy: f64,
    bit_width: f64,
}

impl ClusterPoint {
    pub fn coords(&self) -> [f64;5] {
        [self.var, self.act, self.ham_act, self.entropy, self.bit_width]
    }
    pub fn set_coords(&mut self, coords: [f64;5]) {
        self.var = coords[0];
        self.act = coords[1];
        self.ham_act = coords[2];
        self.entropy = coords[3];
        self.bit_width = coords[4];
    }
}

fn normalize(points: &mut [ClusterPoint]) {
    let means:Vec<f64> = 
        [0.0;5].into_iter().enumerate().map(|(i,_)| points.iter().map(|p| p.coords()[i]).sum::<f64>() / points.len() as f64).collect();
    let stds:Vec<f64> = 
        [0.0;5].into_iter().enumerate()
        .map(|(i,_)| (points.iter().map(|p| (p.coords()[i] - means[i]).powf(2.0)).sum::<f64>() / points.len() as f64).sqrt()).collect();
    for p in points {
        let coords = p.coords();
        p.set_coords([
            (coords[0] - means[0]) / stds[0],
            (coords[1] - means[1]) / stds[1],
            (coords[2] - means[2]) / stds[2],
            (coords[3] - means[3]) / stds[3],
            (coords[4] - means[4]) / stds[4]
        ]);

    }
}

pub struct Cluster {
    centroid: [f64;5],
    members: Vec<usize>
}

fn distance_sq(point: &[f64;5], centroid: &[f64;5]) -> f64 {
    point.iter().zip(centroid).map(|(x,y)| (x-y)*(x-y)).sum()
}

fn kmeans(points: &[ClusterPoint], k: usize, iterations: usize) -> Vec<Cluster> {
    assert!(!points.is_empty());
    assert!(k > 0);
    assert!(k <= points.len());

    let mut clusters = Vec::new();
    clusters.push(Cluster {
        centroid: points.choose(&mut rand::rng()).unwrap().coords(),
        members: Vec::new()
    });
    for _ in 0..(k-1) {
        let mut distances = Vec::new();
        for point in points {
            let min_dist = clusters.iter().map(|x| distance_sq(&point.coords(), &x.centroid)).min_by(|x,y| x.partial_cmp(y).unwrap());
            distances.push(min_dist);
        }
        let sum_sq:f64 = distances.iter().flatten().map(|x| x * x).sum();
        let probs = distances.into_iter().flatten().map(|x| (x * x) / sum_sq).collect::<Vec<f64>>();
        let weights = WeightedIndex::new(probs).unwrap();
        clusters.push(Cluster {
            centroid: points[weights.sample(&mut rand::rng())].coords(),
            members: Vec::new()
        })

    }

    for _ in 0..iterations {
        for cluster in &mut clusters {
            cluster.members.clear();
        }
        for (idx, point) in points.iter().enumerate() {
            let mut best_cluster = 0;
            let mut best_distance = distance_sq(&point.coords(), &clusters[0].centroid);
            for (cluster_idx, cluster) in clusters.iter().enumerate().skip(1) {
                let distance = distance_sq(&point.coords(), &cluster.centroid);
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
            let mut sums = [0.0;5];
            for &member_idx in &cluster.members {
                let coords = &points[member_idx].coords();
                for i in 0..(sums.len()) {
                    sums[i] += coords[i];
                }
            }
            let n = cluster.members.len() as f64;
            let mut centroid = [0.0;5];
            for i in 0..(centroid.len()) {
                centroid[i] += sums[i] / n;
            }
            cluster.centroid = centroid;
        }
    }
    clusters
}

fn inertia(clusters: &[Cluster], points: &[ClusterPoint]) -> f64 {
    clusters.iter().flat_map(|cluster| {
        cluster.members.iter().map(|&i| distance_sq(&points[i].coords(), &cluster.centroid))
    }).sum()
}

fn optimal_k(points: &[ClusterPoint], iterations: usize) -> usize {
    let max_k = (points.len().isqrt() * 2).max(8);
    let min_k = ((points.len() as f64).cbrt() as usize).max(8);

    let inertias: Vec<f64> = (1..=max_k)
        .map(|k| inertia(&kmeans(points, k, iterations), points))
        .collect();

    let inertia_min = inertias.last().cloned().unwrap_or(0.0);
    let inertia_max = inertias.first().cloned().unwrap_or(0.0);
    let n = inertias.len();

    let knee = inertias.iter()
        .enumerate()
        .map(|(i, inertia)| {
            let k_norm = i as f64 / (n - 1) as f64;
            let inertia_norm = (inertia - inertia_min) / (inertia_max - inertia_min);
            inertia_norm - k_norm
        })
        .enumerate()
        .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
        .map(|(i, _)| i + 1) // k is 1-indexed
        .unwrap();

    knee.max(min_k).min(max_k)
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
                score: f.score,
                var: f.variance,
                act: if f.n > 1 {
                    f.toggle_count as f64 / (self.obv_seen - 1) as f64
                } else {
                    0.0
                },
                ham_act:f.ham_toggles as f64 / (self.obv_seen - 1) as f64,
                entropy: f.entropy(),
                bit_width: WIDTHS.lock().unwrap()[&f.key] as f64
            })
            .collect::<Vec<_>>();
        normalize(&mut points);
        let k = optimal_k(&points, 10);
        let clusters = kmeans(&points, k, 20);
        (points, clusters)

    }
    pub fn top_features(&self, limit: usize) -> Vec<String> {
        let (points, clusters) = self.build_clusters();
        let mut selected: Vec<(String, f64)> = Vec::new();
        for (i, cluster) in clusters.iter().enumerate() {
            println!("Cluster {i}");

            for &idx in &cluster.members {
                println!("  {}", NAMES.lock().unwrap()[&points[idx].feature]);
            }
        }
        for cluster in clusters {
            let best = cluster.members.iter().max_by(|&&a, &&b| {
                let da = distance_sq(&points[a].coords(), &cluster.centroid);
                let db = distance_sq(&points[b].coords(), &cluster.centroid);

                da.partial_cmp(&db).unwrap()
            });
            if let Some(&idx) = best {
                selected.push((points[idx].feature.clone(), points[idx].score))
            };
        }
        selected.sort_by(|a, b| {
            b.1.partial_cmp(&a.1).unwrap()
        });
        selected.into_iter().take(limit).map(|(name, _)| name).collect()
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
