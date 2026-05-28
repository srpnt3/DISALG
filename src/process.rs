use std::collections::{HashMap, HashSet};
use message_io::network::{Endpoint, NetEvent, ResourceId};
use message_io::node::{NodeHandler, NodeListener};
use serde::{Deserialize, Serialize};
use crate::algorithm::{algorithm, Message, State};

#[derive(Serialize, Deserialize, Debug)]
pub enum NetMessage {
    Identify(ProcessID),
    Ready(ProcessID),
    Message(Message)
}

impl Process {
    pub fn new(handler: NodeHandler<()>, listener: NodeListener<()>, id: i32, neighbour_ids: Vec<i32>, neighbours_endpoints: Vec<Endpoint>) -> Process {

        Process {
            state: State::default(),
            handler,
            listener: Some(listener),
            id,
            neighbour_ids: HashMap::new(),
            neighbour_endpoints: neighbour_ids.into_iter().zip(neighbours_endpoints).collect()
        }
    }

    fn try_ready(&mut self, ready: &mut HashSet<ProcessID>, c: i32, i: i32) {
        let l = self.neighbour_endpoints.len() as i32;
        if c == l && i == l {
            ready.insert(self.id.clone());
            self.neighbour_endpoints.iter().for_each(|(_, e)| {
                ready.iter().for_each(|id| {
                    self.handler.network().send(*e, &bincode::serialize(&NetMessage::Ready(*id)).unwrap());
                })
            })
        }
    }

    pub fn run(mut self, start: bool, num_processes: i32) {

        let listener = self.listener.take().unwrap();

        let mut connected = 0;
        let mut identified = 0;
        let mut ready: HashSet<ProcessID> = HashSet::new();

        listener.for_each(move |event| match event.network() {
            NetEvent::Message(endpoint, data) => {
                let message: NetMessage = bincode::deserialize(&data).unwrap();

                match message {
                    NetMessage::Identify(id) => {
                        self.neighbour_ids.insert(endpoint, id);
                        identified = identified + 1;
                        self.try_ready(&mut ready, connected, identified)
                    }
                    NetMessage::Ready(id) => {
                        if !ready.contains(&id) {
                            ready.insert(id);

                            if (ready.len() as i32 == num_processes) && start {
                                algorithm(&mut self, (Message::Start, -1));
                                println!("Start() process {}", self.id);
                            }

                            self.neighbour_endpoints.iter().for_each(|(_, e)| {
                                self.handler.network().send(*e, data);
                            });
                        }
                    }
                    NetMessage::Message(message) => {
                        if let Some(process_id) = self.neighbour_ids.get(&endpoint).cloned() {
                            println!("Received message {:?} from process {process_id}", message);
                            algorithm(&mut self, (message, process_id));
                        } else {
                            println!("Received message from unknown endpoint {endpoint}");
                        }
                    }
                }
            }
            NetEvent::Connected(endpoint, _) => {
                self.handler.network().send(endpoint, &bincode::serialize(&NetMessage::Identify(self.id)).unwrap());
                connected = connected + 1;
                self.try_ready(&mut ready, connected, identified)
            }
            _ => {}
        });
    }

    pub fn send(&self, message: Message, process_id: ProcessID) {
        if let Some(endpoint) = self.neighbour_endpoints.get(&process_id) {
            println!("Sending message {:?} to process {process_id}", message);
            self.handler.network().send(*endpoint, &bincode::serialize(&NetMessage::Message(message)).unwrap());
        } else {
            println!("Cannot send, did not find endpoint for process {process_id}");
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
    id: ProcessID,
    pub state: State
}