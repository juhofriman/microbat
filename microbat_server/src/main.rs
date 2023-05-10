use connect::MicrobatServerOpts;

mod sql;
mod connect;

fn main() {
    connect::run_microbat(MicrobatServerOpts {
       bind: String::from("127.0.0.1:7878"), 
    })
}
