mod client;
mod render_result;
mod repl;

use crate::client::{MicroBatTcpClient, MicrobatClientOpts};
use crate::repl::MicrobatREPL;

/// Boot up microbat client
fn main() {
    match MicroBatTcpClient::connect(MicrobatClientOpts {
        host: String::from("localhost"),
        port: 7878,
    }) {
        Ok(client) => {
            let mut repl = MicrobatREPL::new(client);
            repl.run();
        }
        Err(err) => {
            println!("FATAL: {}", err.msg)
        }
    }
}
