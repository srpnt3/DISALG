use message_io::network::{Endpoint, NetEvent};
use message_io::node::{NodeHandler, NodeListener};
use crate::algorithm::{algorithm, Message, State};

impl Process {
    pub fn new(handler: NodeHandler<()>, listener: NodeListener<()>, id: i32, neighbours: Vec<Endpoint>, start: bool) -> Process {
        Process {
            start,
            state: State::default(),
            handler,
            listener: Some(listener),
            id,
            neighbours
        }
    }

    pub fn run(mut self) {

        let listener = self.listener.take().unwrap();
        let mut accepted = 0;

        listener.for_each(move |event| match event.network() {
            NetEvent::Message(endpoint, data) => {
                let message: Message = bincode::deserialize(&data).unwrap();
                algorithm(&mut self, (message, endpoint));
            }
            NetEvent::Accepted(endpoint, _) => {
                accepted = accepted + 1;
                if accepted == self.neighbours.len() && self.start {
                    algorithm(&mut self, (Message::Start, endpoint));
                }
            }
            _ => {}
        });
    }

    pub fn send(&self, message: Message, endpoint: Endpoint) {
        self.handler.network().send(endpoint, &bincode::serialize(&message).unwrap());
    }
}

pub struct Process {
    handler: NodeHandler<()>,
    listener: Option<NodeListener<()>>,
    start: bool,
    pub id: i32,
    pub neighbours: Vec<Endpoint>,
    pub state: State
}