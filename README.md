# Daedalus
Streaming feature selection for VCD register data.

```
Usage: daedalus [OPTIONS] [FILES]...

Arguments:
  [FILES]...  The verilog files to run Icarus Verilog on

Options:
      --ivl-suffix <SUFFIX>  The suffix for the iverilog executable [default: ""]
      --ivl-path <PATH>      The path to the iverilog and vvp exectuables
      --ivl-args <ARGS>      Arguments provided to the iverilog exectuable [default: ""]
  -o, --ivl-out <FILE>       The output file for iverilog, equivalent to setting ivl_args="-o a.vvp" [default: a.vvp]
      --vvp-args <ARGS>      Arguments provided to the vvp executable [default: ""]
      --vvp-ext-args <ARGS>  Extended arguments provided to the vvp exectuable [default: -stream]
  -s, --server <SERVER>      The broker that the Kafka consumer will listen at [default: localhost:9092]
  -t, --topic <TOPIC>        The topic the consumer should listen to [default: iv_data_stream]
  -d, --delay <DELAY>        The delay (in seconds) before vvp runs, increase this if the consumer is failing to read data [default: 0]
  -l, --listen               Enables a listen mode that takes no file arguments and instead only listens to an outside data stream, all arguments relating to iverilog and vvp will be ignored
  -v, --vvp                  Enables a mode that takes a pre-compiled .vvp file from iverilog, ignores all iverilog options
  -h, --help                 Print help
  -V, --version              Print version
```

Requires [iverilog_stream](https://github.com/Arcturus-The-Star/iverilog_stream), [Apache Kafka](https://kafka.apache.org/) v4.3.0 or greater, and [librdkafka](https://github.com/confluentinc/librdkafka) v2.14.2 or greater. In order to function, a Kafka broker must be started with a topic called `iv_data_stream` (Pending further changes to iverilog_stream).

Building from source requires rustc 1.96.0 and cargo 1.96.0, run the command `cargo build --release` to build, the executable will be located in `/target/release/daedalus`.
