use daedalus::*;
use core::sync::atomic::Ordering;
use std::{path::PathBuf, process::exit, sync::mpsc::{channel}, thread};
use clap::Parser;

/// Streaming feature selection for VCD register data
#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Args {
    /// The suffix for the iverilog executable
    #[arg(long, default_value="", value_name="SUFFIX")]
    ivl_suffix: String,
    /// The path to the iverilog and vvp exectuables
    #[arg(long, value_name="PATH")]
    ivl_path: Option<PathBuf>,
    /// Arguments provided to the iverilog exectuable
    #[arg(long, default_value="", value_name="ARGS")]
    ivl_args: Vec<String>,
    /// The output file for iverilog, equivalent to setting ivl_args="-o a.vvp"
    #[arg(long, short='o', default_value="a.vvp", value_name="FILE")]
    ivl_out: PathBuf,
    /// Arguments provided to the vvp executable
    #[arg(long, default_value="", value_name="ARGS")]
    vvp_args: Vec<String>,
    /// Extended arguments provided to the vvp exectuable
    #[arg(long, default_value="-stream", value_name="ARGS")]
    vvp_ext_args: Vec<String>,
    /// The broker that the Kafka consumer will listen at
    #[arg(long, short, default_value="localhost:9092")]
    server: String,
    /// The topic the consumer should listen to
    #[arg(long, short, default_value="iv_data_stream")]
    topic: String,
    /// The delay (in seconds) before vvp runs, increase this if the consumer is failing to read
    /// data
    #[arg(long, short, default_value_t=0)]
    delay: u64,
    /// Enables a listen mode that takes no file arguments and instead only listens to an outside
    /// data stream, all arguments relating to iverilog and vvp will be ignored
    #[arg(short, long)]
    listen: bool,
    /// Enables a mode that takes a pre-compiled .vvp file from iverilog, ignores all iverilog
    /// options
    #[arg(short, long)]
    vvp: bool,
    /// The verilog files to run Icarus Verilog on
    files: Vec<PathBuf>

}

fn main() {
    let args = Args::parse();
    if args.files.is_empty() && !args.listen{
        eprintln!("No files provided");
        exit(1);
    }
    let (snd, recv) = channel();
    let (feat_snd, feat_recv) = channel();
    let consumer = thread::spawn(move || kafka_consumer(&args.server, &args.topic, snd, feat_snd));
    let features = thread::spawn(move || feature_select(feat_recv));
    if !args.listen {
        let path = args.ivl_path.unwrap_or("".into());
        if !args.vvp {
            match run_ivl(&args.files, &args.ivl_out, args.ivl_args, &path, &args.ivl_suffix) {
                Err(e) => {
                    eprintln!("Error running iverilog: {e}");
                    exit(1);
                }
                Ok(o) => {
                    if let Ok(stdout) = std::str::from_utf8(&o.stdout) && !stdout.is_empty(){
                        println!("{stdout}");
                    }
                    if let Ok(stderr) = std::str::from_utf8(&o.stderr) && !stderr.is_empty(){
                        println!("{stderr}");
                    }
                    if !o.status.success() {
                        exit(o.status.code().unwrap_or(1));
                    }
                }
            }
        }
        let file = if args.vvp {&args.files[0]} else {&args.ivl_out};
        let _ = recv.recv(); // Block until consumer thread is ready 
        thread::sleep(std::time::Duration::from_secs(args.delay)); // The consumer needs this to reliably start (for some reason)
        match run_vvp(&path, file, args.vvp_args, args.vvp_ext_args) {
            Err(e) => {
                eprintln!("Error running vvp: {e}");
                exit(1);
            }
            Ok(o) => {
                if let Ok(stdout) = std::str::from_utf8(&o.stdout) && !stdout.is_empty(){
                    println!("{stdout}");
                }
                if let Ok(stderr) = std::str::from_utf8(&o.stderr) && !stderr.is_empty(){
                    println!("{stderr}");
                }
                if !o.status.success() {
                    exit(o.status.code().unwrap_or(1));
                }
            }
        }
    }
    consumer.join().unwrap();
    SHUTDOWN.swap(true, Ordering::Relaxed);
    let selected = features.join().unwrap();
    let names = NAMES.lock().unwrap();
    println!("Final selections:");
    for s in &selected {
        if let Some(s) = names.get(s) {
            println!("  {s}");
        }
    }
}
