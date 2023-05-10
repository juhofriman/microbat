use crate::client::MicroBatTcpClient;
use crate::render_result::QueryExecutionResult;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::{DefaultEditor, Editor};

pub struct MicrobatREPL {
    client: MicroBatTcpClient,
    rl: Editor<(), DefaultHistory>,
}

impl MicrobatREPL {
    pub fn new(client: MicroBatTcpClient) -> MicrobatREPL {
        MicrobatREPL {
            client,
            rl: DefaultEditor::new().unwrap(),
        }
    }

    pub fn run(&mut self) {
        loop {
            match self.rl.readline("microbat> ") {
                Ok(line) => self.execute_query(line),
                Err(ReadlineError::Interrupted) => {
                    println!("CTRL-C");
                    self.client.disconnect().unwrap();
                    println!("Disconnected");
                    break;
                }
                Err(ReadlineError::Eof) => match self.client.handshake() {
                    Ok(_) => {
                        println!("Handshake OK [{}]", self.client.describe());
                    }
                    Err(err) => {
                        println!("Error: {:?}", err);
                        match self.client.disconnect() {
                            Ok(_) => {}
                            Err(err) => {
                                println!("Error: {}", err.msg);
                            }
                        };
                        break;
                    }
                },
                Err(err) => {
                    println!("Error: {}", err);
                    break;
                }
            }
        }
    }

    fn execute_query(&mut self, line: String) {
        match self.client.query(line) {
            Ok(result) => match result {
                QueryExecutionResult::DataTable(result) => {
                    println!("{}", result);
                }
                QueryExecutionResult::Mutation(result) => {
                    println!("{}", result);
                }
            },
            Err(err) => {
                println!("ERROR: {}", err.msg);
            }
        }
    }
}
