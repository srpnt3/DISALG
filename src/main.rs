mod process;
mod algorithm;

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Error, stdin};
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::Duration;
use config::Config;
use message_io::network::Transport;
use rand::prelude::SliceRandom;
use message_io::node::{self};
use serde::Deserialize;
use crate::process::{NetMessage, Process};

#[derive(Deserialize, Debug)]
struct TopologyConfig {
    leader: bool,
    processes: i32,
    connections: Vec<(i32, i32)>,
}

fn spawn_all()/* -> Result<(), String>*/ {

    // read topology
    let topology = Config::builder()
        .add_source(config::File::with_name("topology"))
        .build().unwrap()
        .try_deserialize::<TopologyConfig>().unwrap();

    // random ports (channels) for each process
    let mut pool: Vec<i32> = (3100..3300).collect();
    pool.shuffle(&mut rand::rng());
    let ports: Vec<i32> = pool.into_iter().take(topology.processes as usize).collect();

    println!();
    println!("Distributed network with {} processes", topology.processes);
    println!();

    // spawn each process
    let exe = std::env::current_exe().unwrap();
    let mut children = vec![];
    for i in 0..topology.processes {

        // collect args
        let port = ports[i as usize];
        let neighbours: Vec<i32> = topology.connections.iter()
            .filter_map(|&(x, y)| (x == i).then_some(y).or((y == i).then_some(x)))
            .collect::<Vec<_>>();
        let neighbour_ports = neighbours.iter()
            .map(|&x| ports[x as usize])
            .collect::<Vec<_>>();
        let start = !(topology.leader && i > 0);

        // spawn
        let mut child = Command::new(&exe)
            .arg("process")
            .arg(i.to_string())
            .arg(start.to_string())
            .arg(port.to_string())
            .arg(neighbours.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","))
            .arg(neighbour_ports.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","))
            .arg(topology.processes.to_string())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                println!("[process#{i}/{port}] {}", line.unwrap());
            }
        });

        children.push(child);
    }

    let (handler, listener) = node::split::<()>();
    let (a, b) = handler.network().listen(Transport::FramedTcp, "127.0.0.1:0".to_string()).unwrap();

    let endpoints = ports.iter().enumerate().map(|(i, p)|
        (i, handler.network().connect(Transport::FramedTcp, format!("127.0.0.1:{p}")).unwrap().0)
    ).collect::<HashMap<_, _>>();

    sleep(Duration::from_millis(1000));
    println!("Start typing commands");
    println!("Usage: quit | send <id> <msg>");

    let mut running = true;
    while running {
        let mut input = String::new();
        stdin().read_line(&mut input).expect("");
        let input = input.trim();
        let mut args = input.split_whitespace().collect::<VecDeque<_>>();
        match args.pop_front().unwrap_or("") {
            "quit" => {
                running = false;
                endpoints.iter().for_each(|(_, &e)| {
                    handler.network().send(e, &bincode::serialize(&NetMessage::External("quit".to_string())).unwrap());
                })
            }
            "send" => {
                let id: Option<usize> = args.pop_front().and_then(|x| x.parse().ok());
                let msg = args.pop_front();
                if let Some(&e) = id.and_then(|id| endpoints.get(&id)) && let Some(msg) = msg {
                    handler.network().send(e, &bincode::serialize(&NetMessage::External(msg.to_string())).unwrap());
                } else {
                    println!("Usage: quit | send <id> <msg>");
                }
            }
            _ => {
                println!("Usage: quit | send <id> <msg>");
            }
        }
    }

    for mut c in children {
        c.wait().unwrap();
    }
}

fn spawn_individual(args: Vec<String>) -> Result<(), Error>  {
    let id = args.get(2).unwrap().parse::<i32>().unwrap();
    let start = args.get(3).unwrap().parse::<bool>().unwrap();
    let port = args.get(4).unwrap().parse::<i32>().unwrap();
    let neighbour_ids = args.get(5).unwrap().split(',').map(|x| x.parse().unwrap()).collect::<Vec<_>>();
    let neighbour_ports = args.get(6).unwrap().split(',').collect::<Vec<_>>();
    let num_procs = args.get(7).unwrap().parse::<i32>().unwrap();

    let (handler, listener) = node::split::<()>();

    let (a,b) = handler.network().listen(Transport::FramedTcp, format!("127.0.0.1:{port}"))?;

    let neighbour_endpoints = neighbour_ports.iter()
        .map(|&port| Ok(handler.network().connect(Transport::FramedTcp, format!("127.0.0.1:{port}"))?.0))
        .collect::<Result<Vec<_>, Error>>()?;

    let process = Process::new(handler, listener, id, neighbour_ids, neighbour_endpoints);
    process.run(start, num_procs);

    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).unwrap_or(&String::new()).as_ref() {
        "" => {
            spawn_all();
        }
        "process" => {
            match spawn_individual(args) {
                Ok(_) => {}
                Err(err) => { println!("{}", err) }
            }
        }
        _ => { println!("Usage: No args pls :)") }
    }
}