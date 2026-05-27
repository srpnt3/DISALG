mod process;
mod algorithm;

use std::io::{BufRead, BufReader, Error};
use std::process::{Command, Stdio};
use config::Config;
use message_io::network::Transport;
use rand::prelude::SliceRandom;
use message_io::node::{self};
use serde::Deserialize;
use crate::process::Process;

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

    let (handler, listener) = node::split::<()>();

    let (_, _) = handler.network().listen(Transport::FramedTcp, format!("127.0.0.1:{port}"))?;

    let neighbour_endpoints = neighbour_ports.iter()
        .map(|&port| Ok(handler.network().connect(Transport::FramedTcp, format!("127.0.0.1:{port}"))?.0))
        .collect::<Result<Vec<_>, Error>>()?;

    let process = Process::new(handler, listener, id, neighbour_ids, neighbour_endpoints, start);
    process.run();

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