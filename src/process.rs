use std::collections::{HashMap};
use message_io::network::{Endpoint, NetEvent};
use message_io::node::{NodeHandler, NodeListener};
use crate::algorithm::{algorithm, Message, State};

impl Process {
    pub fn new(handler: NodeHandler<()>, listener: NodeListener<()>, id: i32, neighbour_ids: Vec<i32>, neighbours_endpoints: Vec<Endpoint>, start: bool) -> Process {
        Process {
            start,
            state: State::default(),
            handler,
            listener: Some(listener),
            id,
            neighbour_ids: neighbours_endpoints.clone().into_iter().zip(neighbour_ids.clone()).collect(),
            neighbour_endpoints: neighbour_ids.into_iter().zip(neighbours_endpoints).collect()
        }
    }

    pub fn run(mut self) {

        let listener = self.listener.take().unwrap();
        let mut accepted = 0;

        listener.for_each(move |event| match event.network() {
            NetEvent::Message(endpoint, data) => {
                let message: Message = bincode::deserialize(&data).unwrap();

                if let Some(process_id) = self.neighbour_ids.get(&endpoint).cloned() {
                    algorithm(&mut self, (message, process_id));
                }

            }
            NetEvent::Accepted(endpoint, _) => {
                accepted = accepted + 1;
                if accepted == self.neighbour_ids.len() && self.start {
                    algorithm(&mut self, (Message::Start, -1));
                }
            }
            _ => {}
        });
    }

    pub fn send(&self, message: Message, process_id: ProcessID) {
        if let Some(endpoint) = self.neighbour_endpoints.get(&process_id) {
            self.handler.network().send(*endpoint, &bincode::serialize(&message).unwrap());
        }
    }

    pub fn neighbours(&self) -> Vec<i32> {
        self.neighbour_endpoints.keys().cloned().collect()
    }

    pub fn id(&self) -> i32 {
        self.id
    }
}

pub type ProcessID = i32;

pub struct Process {
    handler: NodeHandler<()>,
    listener: Option<NodeListener<()>>,
    neighbour_endpoints: HashMap<i32, Endpoint>,
    neighbour_ids: HashMap<Endpoint, i32>,
    start: bool,
    id: ProcessID,
    pub state: State
}