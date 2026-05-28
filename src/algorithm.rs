// If you want to try it out and run it, the full project is on GitHub:
// https://github.com/srpnt3/DISALG/tree/framework

use serde::{Deserialize, Serialize};
use crate::process::{Process, ProcessID};

// Process state
#[derive(Serialize, Deserialize, Default, Debug)]
pub struct State {

}

// Message types
#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Start,
}

// Algorithm
pub fn algorithm(process: &mut Process, msg: (Message, ProcessID)) {
    match msg {

        (Message::Start, _) => {
            println!("Start received")
        }

    }
}