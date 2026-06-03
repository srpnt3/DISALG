// If you want to try it out and run it, the full project is on GitHub:
// https://github.com/srpnt3/DISALG/tree/PROG03

use std::collections::VecDeque;
use serde::{Deserialize, Serialize};
use crate::process::{Process, ProcessID};

// Process state
#[derive(Serialize, Deserialize, Default, Debug)]
pub struct State {
    parent: Option<ProcessID>,
    object: Option<String>,
    interested: bool,
    queue: VecDeque<ProcessID>,

    st_expected: usize,
}

// Message types
#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    RaymondStart(),
    Request(ProcessID),
    Object(String),

    Start,
    StGo(),
    StBack(bool),
}

// like acquire but without waiting, instead we get notified
pub fn request_object(process: &mut Process) {
    process.state.interested = true;
    if let Some(parent) = process.state.parent && parent != process.id() {
        process.state.queue.push_back(process.id());
        if process.state.queue.len() == 1 {
            process.send(Message::Request(process.id()), parent)
        }
    }
}

pub fn release_object(process: &mut Process) {
    process.state.interested = false;
    if let Some(k) = process.state.queue.pop_front() {
        let object = process.state.object.take().unwrap();
        process.send(Message::Object(object), k);
        process.state.parent = Some(k);
        if !process.state.queue.is_empty() {
            process.send(Message::Request(process.id()), k);
        }
    }
}

pub fn received_object(process: &mut Process) {
    let object = process.state.object.take().unwrap();
    println!("Received object: {}", object);
    process.state.object = Some(object);

}

// Algorithm
pub fn algorithm(process: &mut Process, msg: (Message, ProcessID)) {
    match msg {

        (Message::RaymondStart(), _) => {
            process.state.object = Some("Bogus".to_string());
        }

        (Message::Request(k), _) => {
            if let Some(parent) = process.state.parent && parent == process.id() {
                if process.state.interested { process.state.queue.push_back(k) }
                else {
                    let object = process.state.object.take().unwrap();
                    process.send(Message::Object(object), k);
                    process.state.parent = Some(k);
                }
            } else {
                process.state.queue.push_back(k);
                if process.state.queue.len() == 1 && let Some(parent) = process.state.parent {
                    process.send(Message::Request(process.id()), parent);
                }
            }
        }

        (Message::Object(obj), _) => {
            let k = process.state.queue.pop_front().unwrap();
            process.state.parent = Some(k);
            if process.id() == k {
                process.state.object = Some(obj);
                received_object(process);
            } else {
                process.send(Message::Object(obj), k);
                if !process.state.queue.is_empty() {
                    process.send(Message::Request(process.id()), k);
                }
            }
        }

// Anything below is not relevant
        (Message::Start, _) => {
            process.state.parent = Some(process.id());
            process.state.st_expected = process.neighbours().len();
            process.neighbours().iter().for_each(|&p_k| {
                process.send(Message::StGo(), p_k);
            });
            st_local_term(process);
        }
        (Message::StGo(), p_j) => {
            match process.state.parent {
                None => {
                    process.state.parent = Some(p_j);
                    process.state.st_expected = process.neighbours().len() - 1;
                    process.neighbours().iter().filter(|&&p_k| p_k != p_j).for_each(|&p_k| {
                        process.send(Message::StGo(), p_k);
                    });
                }
                Some(_) => {
                    process.send(Message::StBack(false), p_j);
                }
            }
            st_local_term(process);
        }
        (Message::StBack(b), p_j) => {
            process.state.st_expected -= 1;
            //if b { process.state.children.push(p_j); }
            st_local_term(process);
        }
    }
}

pub fn st_local_term(process: &mut Process) {
    if process.state.st_expected == 0 {
        if let Some(p) = process.state.parent && p != process.id() {
            process.send(Message::StBack(true), p);
        } else {
            algorithm(process, (Message::RaymondStart(), 0));
        }
    }
}