use std::os::fd::FromRawFd;
use std::os::unix::prelude::OwnedFd;
use std::process::{Command, Stdio, exit};

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
            while ipc.recv(|data: String| {
                if rand::random_bool(0.1) {
                    // Randomly terminate the process without providing a response,
                    // to simulate, for example, the process being killed by the OOM killer,
                    // or crashing because of a segfault in a linked C library.
                    exit(1);
                }

                format!("Hello, {data}!")
            }) {}
        },
    }
}
