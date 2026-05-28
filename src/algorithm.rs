use std::collections::HashSet;
use serde::{Deserialize, Serialize};
use crate::process::{Process, ProcessID};

pub type ValSet = HashSet<(ProcessID, String)>;

// Process state
#[derive(Serialize, Deserialize, Debug)]
pub struct State {
    pub parent: ProcessID,
    pub children: HashSet<ProcessID>,
    pub expected_msgs: i32,
    pub val_set: ValSet,
}
impl State {
    pub fn default() -> State {
        State {
            parent: -1,
            children: HashSet::new(),
            expected_msgs: 0,
            val_set: HashSet::new(),
        }
    }
}

// Message types
#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Start,
    Go(String),
    Back(ValSet),
}

// Algorithm
pub fn algorithm(process: &mut Process, msg: (Message, ProcessID)) {

    let id = process.id();
    match msg {
        (Message::Start, _) => {
            process.state.parent = id;
            process.state.expected_msgs = process.neighbours().len() as i32;
            process.neighbours().iter().for_each(|p_j| {
                process.send(Message::Go("Data".to_string()), *p_j);
            });
        },

        (Message::Go(data), p_j) => {
            if process.state.parent == -1 {
                process.state.parent = p_j;
                process.state.expected_msgs = process.neighbours().len() as i32 - 1;
                if process.state.expected_msgs == 0 {
                    process.send(Message::Back(ValSet::from([(id, id.to_string())])), p_j);
                } else {
                    process.neighbours().iter()
                        .filter(|&&p_k| p_k != p_j)
                        .for_each(|&p_k| process.send(Message::Go(data.clone()), p_k));
                }
            } else {
                process.send(Message::Back(HashSet::default()), p_j);
            }
        },

        (Message::Back(val_set), p_j) => {
            process.state.expected_msgs = process.state.expected_msgs - 1;
            if !val_set.is_empty() {
                process.state.children.insert(p_j);
                process.state.val_set.extend(val_set.iter().cloned());
            }
            if process.state.expected_msgs == 0 {
                process.state.val_set.insert((id, id.to_string()));
                if id != process.state.parent {
                    process.send(Message::Back(process.state.val_set.clone()), process.state.parent);
                } else {
                    println!("DONE! val_set: {:?}", process.state.val_set);
                }
            }
        }
    }
}