# `simple-ipc-worker`

This crate provides boilerplate needed to run work in worker processes.
It provides IPC, automatic periodic restarts and retries.

The goal of this crate is twofold:

- Run libraries in parallel even if they don't support multi-threading well.
- Deal with unexpected crashes, for example when a process OOMs.

For example, if you try SAT-checking many simple problems with Z3, naively
parallelizing it can lead to >90% of CPU-time being lost in lock contention.
Additionally, Z3 can sometimes use multiple gigabytes of memory, which can
cause OOMs when run on a server low on RAM.
This normally takes down the entire process, which is undesirable.

# Example
```rust
use std::os::fd::FromRawFd;
use std::os::unix::prelude::OwnedFd;
use std::process::Command;

use clap::Parser;
use simple_ipc_worker::{IpcClient, IpcConfig};

#[derive(Parser)]
struct Args {
    #[clap(long)]
    worker_fd: Option<i32>,
}

fn main() {
    match Args::parse().worker_fd {
        None => {
            let mut worker = IpcConfig::default().create(|fd| {
                let mut cmd = Command::new(std::env::current_exe().unwrap());
                cmd.arg("--worker-fd").arg(fd);
                cmd
            });

            for n in 0..1000 {
                let result: String = worker.send(&format!("world {n}"));
                println!("{result}");
            }
        },
        Some(fd) => {
            // Prefer killing this process over its parent.
            std::fs::write("/proc/self/oom_score_adj", "500\n").unwrap();

            let fd = unsafe { OwnedFd::from_raw_fd(fd) };
            let mut ipc = IpcClient::from_parent_fd(fd);
            while ipc.recv(|data: String| format!("Hello, {data}!")) {}
        },
    }
}
```