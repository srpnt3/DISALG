use message_io::network::Endpoint;
use serde::{Deserialize, Serialize};
use crate::process::Process;

// Process state
#[derive(Serialize, Deserialize, Default)]
pub struct State {

}

// Message types
#[derive(Serialize, Deserialize)]
pub enum Message {
    Start,
}

// Algorithm
pub fn algorithm(_process: &mut Process, msg: (Message, Endpoint)) {
    match msg {
        (Message::Start, _) => {
            println!("Start received")
        }
    }
}