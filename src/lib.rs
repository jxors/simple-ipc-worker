//! This crate provides boilerplate needed to run work in worker processes.
//! It provides IPC, automatic periodic restarts and retries.
//!
//! The goal of this crate is twofold:
//!
//! - Run libraries in parallel even if they don't support multi-threading well.
//! - Deal with unexpected crashes, for example when a process OOMs.
//!
//! For example, if you try SAT-checking many simple problems with Z3, naively
//! parallelizing it can lead to >90% of CPU-time being lost in lock contention.
//! Additionally, Z3 can sometimes use multiple gigabytes of memory, which can
//! cause OOMs when run on a server low on RAM.
//! This normally takes down the entire process, which is undesirable.
//!
//! # Example
//! ```rust
//! use std::os::fd::FromRawFd;
//! use std::os::unix::prelude::OwnedFd;
//! use std::process::Command;
//!
//! use clap::Parser;
//! use simple_ipc_worker::{IpcClient, IpcConfig};
//!
//! #[derive(Parser)]
//! struct Args {
//!     #[clap(long)]
//!     worker_fd: Option<i32>,
//! }
//!
//! fn main() {
//!     match Args::parse().worker_fd {
//!         None => {
//!             let mut worker = IpcConfig::default().create(|fd| {
//!                 let mut cmd = Command::new(std::env::current_exe().unwrap());
//!                 cmd.arg("--worker-fd").arg(fd);
//!                 cmd
//!             });
//!
//!             for n in 0..1000 {
//!                 let result: String = worker.send(&format!("world {n}"));
//!                 println!("{result}");
//!             }
//!         },
//!         Some(fd) => {
//!             // Prefer killing this process over its parent.
//!             std::fs::write("/proc/self/oom_score_adj", "500\n").unwrap();
//!
//!             let fd = unsafe { OwnedFd::from_raw_fd(fd) };
//!             let mut ipc = IpcClient::from_parent_fd(fd);
//!             while ipc.recv(|data: String| format!("Hello, {data}!")) {}
//!         },
//!     }
//! }
//! ```

#![deny(missing_docs)]
use std::io::{ErrorKind, Read, Write};
use std::marker::PhantomData;
use std::net::Shutdown;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

const INITIAL_BUFFER_SIZE: usize = 1024;

/// An IPC client running in the worker process.
pub struct IpcClient<I, O> {
    socket: UnixStream,
    _phantom: PhantomData<(I, O)>,
    buf: Vec<u8>,
}

/// An IPC host that can send work to IPC clients.
pub struct IpcHost<I, O> {
    socket: UnixStream,
    _phantom: PhantomData<(I, O)>,
    builder: Box<dyn FnMut(&str) -> Command>,
    buf: Vec<u8>,
    worker: std::process::Child,
    config: IpcConfig,
    created_at: u64,
}

impl<I: DeserializeOwned, O: Serialize> IpcClient<I, O> {
    /// Creates a client from the provided file descriptor.
    pub fn from_parent_fd(socket: OwnedFd) -> Self {
        Self {
            socket: UnixStream::from(socket),
            buf: vec![0; INITIAL_BUFFER_SIZE],
            _phantom: PhantomData,
        }
    }

    /// Receives one command from the main process.
    /// A single command is read and passed to `recv`.
    /// The value returned by `recv` is sent back to the main process.
    ///
    /// This function returns false when the socket has been closed.
    ///
    /// There should be no global state kept between invocations of `recv`.
    /// The process may be restarted in between invocations,
    /// and the same command may be executed multiple times if errors are encountered.
    pub fn recv(&mut self, recv: impl FnOnce(I) -> O) -> bool {
        match read_into_buffer(&mut self.buf, &mut self.socket) {
            Ok(buf) => {
                let response = recv(pot::from_slice(buf).unwrap());

                let buf = pot::to_vec(&response).unwrap();
                self.socket
                    .write_all(&u32::to_le_bytes(
                        buf.len().try_into().expect("response cannot be bigger than 4GiB"),
                    ))
                    .unwrap();
                self.socket.write_all(&buf).expect("UnixStream::write_all failed");

                true
            },
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => false,
            Err(e) => panic!("Error reading from stream: {e}"),
        }
    }
}

fn read_into_buffer<'buf>(buf: &'buf mut Vec<u8>, stream: &mut impl Read) -> std::io::Result<&'buf [u8]> {
    let mut len_buf = [0; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if buf.len() < len {
        buf.resize(len, 0);
    }

    stream.read_exact(&mut buf[..len])?;

    Ok(&buf[..len])
}

fn time() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    static COUNTER_INCREMENT_THREAD: OnceLock<JoinHandle<()>> = OnceLock::new();

    COUNTER_INCREMENT_THREAD.get_or_init(|| {
        std::thread::Builder::new()
            .name("ipc-worker-age-counter".into())
            .spawn(|| {
                loop {
                    std::thread::sleep(Duration::from_secs(1));
                    COUNTER.fetch_add(1, Ordering::Relaxed);
                }
            })
            .unwrap()
    });

    COUNTER.load(Ordering::SeqCst)
}

/// Configuration for [`IpcHost`].
pub struct IpcConfig {
    max_lifetime_seconds: u64,
    kill_delay: Duration,
    hide_stdout: bool,
    hide_stderr: bool,
}

impl Default for IpcConfig {
    fn default() -> Self {
        Self {
            max_lifetime_seconds: 300,
            kill_delay: Duration::from_secs(1),
            hide_stdout: false,
            hide_stderr: false,
        }
    }
}

impl IpcConfig {
    /// Sets the maximum lifetime of the worker process, in seconds.
    /// Before sending a command to the worker process, the lifetime is checked.
    /// If the lifetime has been exceeded, the process is restarted.
    ///
    /// Setting the lifetime to 0 seconds means the process will be restarted
    /// before each request.
    ///
    /// A lower lifetime increases the overhead of each command.
    pub fn max_lifetime(self, max_lifetime_seconds: u64) -> Self {
        Self {
            max_lifetime_seconds,
            ..self
        }
    }

    /// When terminating a process, there is a grace period that allows the worker
    /// process to shut down cleanly in the background.
    /// After this delay, the process is forcefully killed by calling [`std::process::Child::kill`].
    pub fn kill_delay(self, kill_delay: Duration) -> Self {
        Self {
            kill_delay,
            ..self
        }
    }

    /// Creates a new IPC host from this configuration.
    pub fn create<I: DeserializeOwned, O: Serialize, F: FnMut(&str) -> Command + 'static>(
        self, process_builder: F,
    ) -> IpcHost<I, O> {
        let mut builder = Box::new(process_builder) as Box<dyn FnMut(&str) -> Command>;
        let (socket, worker) = spawn_worker(&mut builder);
        IpcHost {
            socket,
            buf: vec![0; INITIAL_BUFFER_SIZE],
            builder,
            worker,
            created_at: time(),
            config: self,
            _phantom: PhantomData,
        }
    }
}

impl<I: DeserializeOwned, O: Serialize> IpcHost<I, O> {
    fn restart(&mut self) {
        let (mut new_socket, mut new_child) = spawn_worker(&mut self.builder);
        std::mem::swap(&mut self.socket, &mut new_socket);
        std::mem::swap(&mut self.worker, &mut new_child);

        // Wait on the child in the background so it doesn't become a zombie.
        let kill_delay = self.config.kill_delay;
        std::thread::spawn(move || {
            std::thread::sleep(kill_delay);

            new_child.kill().ok();
            new_child.wait().ok();
        });

        self.created_at = time();
    }

    /// Sends a command to the worker process. This operation is synchronous.
    /// It blocks the current thread until a response from the worker process is received.
    pub fn send(&mut self, data: &O) -> I {
        loop {
            let buf = pot::to_vec(&data).unwrap();
            self.send_data(buf);

            match read_into_buffer(&mut self.buf, &mut self.socket) {
                Ok(buf) => return pot::from_slice(&buf).unwrap(),
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                    self.restart();
                },
                Err(e) => panic!("Error reading response: {e}"),
            }
        }
    }

    fn send_data(&mut self, buf: Vec<u8>) {
        loop {
            if self.created_at + self.config.max_lifetime_seconds <= time() {
                self.restart();
            }

            let result = (|| {
                self.socket.write_all(&u32::to_le_bytes(buf.len() as u32))?;
                self.socket.write_all(&buf)?;

                std::io::Result::Ok(())
            })();

            match result {
                Ok(_) => (),
                Err(e) if e.kind() == ErrorKind::BrokenPipe => {
                    self.restart();
                    continue;
                },
                Err(e) => panic!("UnixStream::write_all failed: {e}"),
            }

            break;
        }
    }
}

fn spawn_worker(builder: &mut dyn FnMut(&str) -> Command) -> (UnixStream, std::process::Child) {
    // UnixDatagram sockets require contiguous physical memory for messages.
    // This can be difficult to allocate under memory pressure, leading to errors.
    // To avoid this, we use streams instead, and prefix every message with a message length.
    let (socket, worker_socket) = UnixStream::pair().unwrap();

    let fd_str = worker_socket.as_raw_fd().to_string();
    let mut cmd = builder(&fd_str);
    let worker = unsafe {
        cmd.pre_exec(move || {
            // Clear FD_CLOEXEC.
            libc::fcntl(worker_socket.as_raw_fd(), libc::F_SETFD, 0);
            Ok(())
        })
        .spawn()
        .unwrap()
    };
    (socket, worker)
}

impl<I, O> Drop for IpcHost<I, O> {
    fn drop(&mut self) {
        self.socket.shutdown(Shutdown::Both).ok();
    }
}
