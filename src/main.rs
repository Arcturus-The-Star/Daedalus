use daedalus::*;
use std::{thread, path::PathBuf, process::exit};
use clap::Parser;

/// Placeholder info text
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
    /// The verilog files to run Icarus Verilog on
    files: Vec<PathBuf>

}

fn main() {
    let args = Args::parse();
    if args.files.is_empty() {
        eprintln!("No files provided");
        exit(1);
    }
    match run_ivl(&args.files, &args.ivl_out, args.ivl_args, &args.ivl_path.unwrap_or("".into()), &args.ivl_suffix) {
        Err(e) => {
            eprintln!("Error running iverilog: {e}");
            exit(1);
        }
        Ok(o) => {
            println!("{}", std::str::from_utf8(&o.stdout).unwrap_or(""));
            println!("{}", std::str::from_utf8(&o.stderr).unwrap_or(""));
            if !o.status.success() {
                exit(o.status.code().unwrap_or(1));
            }
        }
    }
    let consumer = thread::spawn(move || kafka_consumer(&args.server));
    consumer.join().unwrap();
    let features = FEATURES.lock().unwrap();
    for reg in features.iter() {
        println!("{reg:?}");
    }
}
