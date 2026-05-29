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
    let n =  process.neighbours().len();

    if process.state.result.is_some() { return; }

    match msg {
        (Message::Start, _) => {
            process.state.inf.insert(process.id());
            process.state.new.insert(process.id());
            process.state.comp_uf = Some(QuickUnionUf::new(n));
            process.state.com_with = HashSet::from_iter(process.neighbours().iter().cloned());
        }

        (Message::MSG(set), x) => {
            process.state.buffer.push((set, x));
        }
    }

    let Some(mut uf) = process.state.comp_uf.take() else {
        return;
    };

    while !process.state.new.is_empty() || process.state.round_active {
        if !process.state.round_active {
            process.state.round_active = true;
            process.state.round += 1;
            process.state.com_with.iter().for_each(|&c| {
                process.send(MSG(process.state.new.clone()), c);
            });
            process.state.new.clear();
            process.state.awaiting = process.state.com_with.clone();
        }

        let mut awaiting = std::mem::take(&mut process.state.awaiting);
        let r = process.state.round;

        awaiting.retain(|&x| {
            let Some((new, x)) = process.state.buffer.iter()
                .position(|&(_, i)| x == i)
                .map(|i| process.state.buffer.remove(i)) else {
                return true;
            };
            if new.is_empty() { process.state.com_with.remove(&x); return false; }

            let aux: Vec<_> = new.iter()
                .filter(|i| !process.state.inf.contains(i) && !process.state.new.contains(i))
                .cloned()
                .collect();

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
        process.state.awaiting = awaiting; // put back in place

        if !process.state.awaiting.is_empty() {
            process.state.comp_uf = Some(uf); // put back in place
            return;
        }

        process.state.inf.extend(process.state.new.iter().cloned());
        process.state.round_active = false;
    }

    process.state.com_with.iter().for_each(|&c| {
        process.send(MSG(HashSet::new()), c);
    });

    process.state.result = Some(n == 0 || (1..n).any(|i| uf.find(i) != uf.find(0)));
    process.state.comp_uf = Some(uf); // put back in place

    if process.state.result.unwrap_or(false) {
        println!("CUT VERTEX");
    }
}
