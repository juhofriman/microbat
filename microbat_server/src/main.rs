use connect::MicrobatServerOpts;

mod connect;
mod db;
mod sql;

fn main() {
    connect::run_microbat(MicrobatServerOpts {
        bind: String::from("127.0.0.1:7878"),
    })
}
