// If you want to try it out and run it, the full project is on GitHub:
// https://github.com/srpnt3/DISALG/tree/PROG02

use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use union_find::{UnionFind, QuickUnionUf, UnionBySize};
use crate::algorithm::Message::MSG;
use crate::process::{Process, ProcessID};

// Process state
#[derive(Default, Debug)]
pub struct State {
    inf: HashSet<ProcessID>,
    new: HashSet<ProcessID>,
    com_with: HashSet<ProcessID>,

    routing_to: HashMap<ProcessID, ProcessID>,
    dist: HashMap<ProcessID, i32>,
    comp_uf: Option<QuickUnionUf<UnionBySize>>,

    round: i32,
    round_active: bool,
    awaiting: HashSet<ProcessID>,
    result: Option<bool>,
    buffer: Vec<(HashSet<ProcessID>, ProcessID)>,
}

// Message types
#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Start,
    MSG(HashSet<ProcessID>),
}

// Algorithm
pub fn algorithm(process: &mut Process, msg: (Message, ProcessID)) {
    let id = process.id();
    let n =  process.neighbours().len();

    if process.state.result.is_some() { return; }

    match msg {
        (Message::Start, _) => {
            process.state.inf.insert(id);
            process.state.new.insert(id);
            process.state.comp_uf = Some(QuickUnionUf::new(n));
            process.state.com_with = HashSet::from_iter(process.neighbours().iter().cloned());
        }

        (Message::MSG(set), id) => {
            process.state.buffer.push((set, id));
        }
    }

    let Some(mut uf) = process.state.comp_uf.take() else {
        return;
    };

    while !process.state.new.is_empty() || process.state.round_active {
        if !process.state.round_active {
            process.state.round_active = true;
            process.state.round = process.state.round + 1;
            process.state.com_with.iter().for_each(|&c| {
                process.send(MSG(process.state.new.clone()), c);
            });
            process.state.new.clear();
            process.state.awaiting = process.state.com_with.clone();
        }

        let mut awaiting = std::mem::take(&mut process.state.awaiting);
        let r = process.state.round;

        awaiting.retain(|&i| {
            let Some((new, x)) = process.state.buffer.iter()
                .position(|&(_, j)| i == j)
                .map(|i| process.state.buffer.remove(i)) else {
                return true;
            };
            if new.is_empty() { process.state.com_with.remove(&x); return false; }

            let aux: HashSet<ProcessID> = new.difference(&process.state.inf.union(&process.state.new).cloned().collect()).cloned().collect();
            new.iter().for_each(|&id| {
                if !process.state.inf.contains(&id) && !process.state.new.contains(&id) {
                    process.state.routing_to.insert(id, x);
                    process.state.dist.insert(id, r);
                } else if let Some(&d) = process.state.dist.get(&id)
                    && (r == d || r == d + 1)
                    && let Some(&y) = process.state.routing_to.get(&id)
                    && let Some(ix) = process.neighbours().iter().position(|&id| id == x)
                    && let Some(iy) = process.neighbours().iter().position(|&id| id == y)
                {
                    uf.union(ix, iy);
                }
            });
            process.state.new.extend(aux);

            false
        });
        process.state.awaiting = awaiting;

        if process.state.awaiting.is_empty() {
            process.state.inf.extend(process.state.new.clone());
            process.state.round_active = false;
        } else {
            process.state.comp_uf = Some(uf);
            return;
        }
    }

    if !process.state.round_active {
        process.state.com_with.iter().for_each(|&c| {
            process.send(MSG(process.state.new.clone()), c);
        });

        process.state.result = Some(n == 0 || (1..n).any(|i| uf.find(i) != uf.find(0)));
        process.state.comp_uf = Some(uf);

        if process.state.result.unwrap_or(false) {
            println!("CUT VERTEX");
        }
    }
}
