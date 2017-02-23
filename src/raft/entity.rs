use std::time;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{channel, Sender, Receiver};

use super::message as message;

// 設定
#[derive(Debug)]
pub struct Setting {
    pub message_count: i32,
    pub server_index: usize,
    pub servers: Vec<(String, u16)>,
    pub receive_thread_loop_wait: time::Duration,
    pub send_thread_loop_wait: time::Duration,
    pub election_timeout: time::Duration,
    pub read_timeout: time::Duration,
}

impl Setting {
    pub fn new(message_count: i32, server_index: usize, servers: &Vec<(String, u16)>) -> Setting {
        Setting { message_count: message_count, server_index: server_index,
            servers: servers.clone(),
            receive_thread_loop_wait: time::Duration::from_millis(1000),
            send_thread_loop_wait: time::Duration::from_millis(1000),
            election_timeout: time::Duration::from_millis(500),
            read_timeout: time::Duration::from_millis(10), }
    }
}

// TODO: ディスクに保存する
#[derive(Debug)]
pub struct PersistentState {
    pub role: Role,
    pub receive_time: time::Instant,
}

impl PersistentState {
    pub fn new() -> PersistentState {
        PersistentState { role: Role::Follower, receive_time: time::Instant::now() }
    }
}

#[derive(Debug)]
pub struct ShareState {
    pub role: Role,
    pub receive_time: time::Instant,
}

impl ShareState {
    pub fn new() -> ShareState {
        ShareState { role: Role::Follower, receive_time: time::Instant::now() }
    }
}

#[derive(Debug)]
pub struct State {
    pub b_finish: AtomicBool,
    pub persistent: Arc<RwLock<PersistentState>>,
    pub shared: Arc<RwLock<ShareState>>,
    pub channels: Vec<(Sender<message::Message>, Receiver<message::Message>)>,
}

unsafe impl Send for State {}
unsafe impl Sync for State {}

impl State {
    pub fn new(length: usize, persistent: &Arc<RwLock<PersistentState>>, shared: &Arc<RwLock<ShareState>>) -> State {
        State { b_finish: AtomicBool::new(false), persistent: persistent.clone(), shared: shared.clone(),
            channels: (0..length).map(|n| channel()).collect::<Vec<_>>() }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}
