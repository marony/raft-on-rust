#![feature(conservative_impl_trait)]

use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, RwLock};
use std::{thread, time};
use std::sync::atomic::Ordering;
use std::net::{SocketAddrV4, Ipv4Addr, SocketAddr};
use std::net::UdpSocket;
use std::str::FromStr;
use std::fmt::Debug;

extern crate rustc_serialize;
extern crate bincode;

use raft::entity as entity;
use super::message as message;

trait RaftLogic : Debug {
    fn get_state(&self) -> Arc<entity::State>;
    fn get_setting(&self) -> Arc<entity::Setting>;

    fn on_role(&self, from_role: entity::Role) -> ();
    fn out_role(&self, to_role: entity::Role) -> ();

    fn on_receive(&self, message: &message::Message, &address: &SocketAddr) -> (bool, Option<entity::Role>);
    fn process(&self) -> (bool, Option<entity::Role>);

    fn on_receive_for_all(&self, message: &message::Message, &address: &SocketAddr) -> (bool, Option<entity::Role>) {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("on_receive_for_all({}, {:?}):", setting.server_index, state.shared.read().unwrap().role);
        }
        (true, None)
    }

    fn process_for_all(&self) -> (bool, Option<entity::Role>) {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("process_for_all({}, {:?}):", setting.server_index, state.shared.read().unwrap().role);
        }
        // TODO: commitIndex, lastAppliedのチェック
        // TODO: 受信時 term, currentTermのチェック
        (true, None)
    }
}

#[derive(Debug)]
struct RaftNode {
    state: Arc<entity::State>,
    setting: Arc<entity::Setting>,
    logic: Option<Box<RaftLogic>>,
}

impl RaftNode {
    pub fn new(state: &Arc<entity::State>, setting: &Arc<entity::Setting>) -> Rc<RefCell<RaftNode>> {
        let node = Rc::new(RefCell::new(RaftNode { state: state.clone(), setting: setting.clone(), logic: None, }));
        let logic = Box::new(Follower { state: state.clone(), setting: setting.clone(), });
        {
            let mut node2 = node.borrow_mut();
            node2.logic = Some(logic);
        }
        node
    }

    fn to_role(&mut self, role: entity::Role) -> () {
        // FIXME: #![feature(conservative_impl_trait)]が効かない
        let state;
        let setting;
        {
            state = self.state.clone();
            setting = self.setting.clone();
        }
        {
            let shared;
            {
                shared = state.shared.read().unwrap();
            }
            info!("{:?} -> {:?}", shared.role, role);
            {
                self.out_role(role);
            }
        }
        {
            let mut shared = state.shared.write().unwrap();
            shared.role = role;
        }
        let logic: Box<RaftLogic> = match role {
            entity::Role::Follower => Box::new(Follower::new(&state, &setting)),
            entity::Role::Candidate => Box::new(Candidate::new(&state, &setting)),
            entity::Role::Leader => Box::new(Leader::new(&state, &setting)),
        };
        {
            self.logic = Some(logic);
        }
        {
            self.on_role(role);
        }
    }

    fn on_role(&self, from_role: entity::Role) -> () {
        let logic = &self.logic.as_ref().unwrap();
        logic.on_role(from_role);
    }

    fn out_role(&self, to_role: entity::Role) -> () {
        let logic = &self.logic.as_ref().unwrap();
        logic.out_role(to_role);
    }

    fn on_receive(&mut self, message: &message::Message, address: &SocketAddr) -> Option<()> {
        let c;
        let op;
        {
            let logic = &self.logic.as_ref().unwrap();
            let t = logic.on_receive(message, address);
            c = t.0;
            op = t.1;
        }
        op.map(|r| self.to_role(r));
        if c { Some(()) } else { None }
    }

    fn process(&mut self) -> Option<()> {
        let c;
        let op;
        {
            let logic = &self.logic.as_ref().unwrap();
            let t = logic.process();
            c = t.0;
            op = t.1;
        }
        op.map(|r| self.to_role(r));
        if c { Some(()) } else { None }
    }

    fn on_receive_for_all(&mut self, message: &message::Message, &address: &SocketAddr) -> Option<()> {
        let c;
        let op;
        {
            let logic = &self.logic.as_ref().unwrap();
            let t = logic.on_receive_for_all(message, &address);
            c = t.0;
            op = t.1;
        }
        op.map(|r| self.to_role(r));
        if c { Some(()) } else { None }
    }

    fn process_for_all(&mut self) -> Option<()> {
        let c;
        let op;
        {
            let logic = &self.logic.as_ref().unwrap();
            let t = logic.process_for_all();
            c = t.0;
            op = t.1;
        }
        op.map(|r| self.to_role(r));
        if c { Some(()) } else { None }
    }
}

#[derive(Debug)]
struct Follower {
    state: Arc<entity::State>,
    setting: Arc<entity::Setting>,
}

impl Follower {
    pub fn new(state: &Arc<entity::State>, setting: &Arc<entity::Setting>) -> Follower {
        let f = Follower { state: state.clone(), setting: setting.clone(), };
        // FIXME: 場所がよくない(on_roleだけでやりたい)
        // 受信時刻を更新
        f.update_receive_time();
        f
    }

    pub fn update_receive_time(&self) {
        // 受信時刻を更新
        let state = self.get_state();
        let mut shared = state.shared.write().unwrap();
        shared.receive_time = time::Instant::now();
    }
}

impl RaftLogic for Follower {
    fn get_state(&self) -> Arc<entity::State> {
        self.state.clone()
    }
    fn get_setting(&self) -> Arc<entity::Setting>
    {
        self.setting.clone()
    }

    fn on_role(&self, from_role: entity::Role) -> () {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("on_role({}, {:?}):", setting.server_index, state.shared.read().unwrap().role);
        }
        // 受信時刻を更新
        self.update_receive_time();
    }
    fn out_role(&self, to_role: entity::Role) -> () {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("out_role({}, {:?}):", setting.server_index, state.shared.read().unwrap().role);
        }
    }

    fn on_receive(&self, message: &message::Message, address: &SocketAddr) -> (bool, Option<entity::Role>) {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("on_receive({}, {:?}): {:?}", setting.server_index, state.shared.read().unwrap().role, message);
        }
        // TODO: AppendEntriesに返事をする
        match *message {
            message::Message::AppendEntries(term, leader_id, prev_log_index, prev_log_term, ref entries, leader_commit) => {
                ()
            },
            message::Message::RequestVote(term, candidate_id, last_log_index, last_log_term) => {
                ()
            },
            message::Message::Test => {
            }
            _ => {}
        };
        (true, None)
    }
    fn process(&self) -> (bool, Option<entity::Role>) {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("process({}, {:?}):", setting.server_index, state.shared.read().unwrap().role);
        }
        // AppendEntriesか選挙がタイムアウトしたらCandidateになる
        let receive_time;
        {
            let state = self.get_state();
            let shared = state.shared.read().unwrap();
            receive_time = shared.receive_time;
        }
        {
            let setting = self.get_setting();
            if time::Instant::now() - receive_time > setting.election_timeout {
                return (false, Some(entity::Role::Candidate));
            }
        }
        (true, None)
    }
}

#[derive(Debug)]
struct Candidate {
    state: Arc<entity::State>,
    setting: Arc<entity::Setting>,
}

impl Candidate {
    pub fn new(state: &Arc<entity::State>, setting: &Arc<entity::Setting>) -> Candidate {
        Candidate { state: state.clone(), setting: setting.clone(), }
    }
}

impl RaftLogic for Candidate {
    fn get_state(&self) -> Arc<entity::State> {
        self.state.clone()
    }
    fn get_setting(&self) -> Arc<entity::Setting>
    {
        self.setting.clone()
    }

    fn on_role(&self, from_role: entity::Role) -> () {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("on_role({}, {:?}):", setting.server_index, state.shared.read().unwrap().role);
        }
        // TODO: currentTerm更新
        {
            let state = self.get_state();
            let mut persistent = state.persistent.write().unwrap();
            persistent.current_term = persistent.current_term + 1;
        }
        // TODO: 自分に投票
        // TODO: election_timeoutをリセット
        // TODO: RequestVote RPCを他のノードに送信
    }
    fn out_role(&self, to_role: entity::Role) -> () {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("out_role({}, {:?}):", setting.server_index, state.shared.read().unwrap().role);
        }
    }

    fn on_receive(&self, message: &message::Message, address: &SocketAddr) -> (bool, Option<entity::Role>) {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("on_receive({}, {:?}):", setting.server_index, state.shared.read().unwrap().role);
        }
        // TODO: マジョリティから投票を受け取ったらLeaderになる
        // TODO: 新しいリーダからAppendEntriesを受け取ったらFollowerになる
        (true, None)
    }
    fn process(&self) -> (bool, Option<entity::Role>) {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("process({}, {:?}):", setting.server_index, state.shared.read().unwrap().role);
        }
        // TODO: election_timeoutしたら新規に選挙を始める
        (true, None)
    }
}

#[derive(Debug)]
struct Leader {
    state: Arc<entity::State>,
    setting: Arc<entity::Setting>,
}

impl Leader {
    pub fn new(state: &Arc<entity::State>, setting: &Arc<entity::Setting>) -> Leader {
        Leader { state: state.clone(), setting: setting.clone(), }
    }
}

impl RaftLogic for Leader {
    fn get_state(&self) -> Arc<entity::State> {
        self.state.clone()
    }
    fn get_setting(&self) -> Arc<entity::Setting>
    {
        self.setting.clone()
    }

    fn on_role(&self, from_role: entity::Role) -> () {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("on_role({}, {:?}):", setting.server_index, state.shared.read().unwrap().role);
        }
        // TODO: 選挙が終わったら最初の空のAppendEntriesを送信する
    }
    fn out_role(&self, to_role: entity::Role) -> () {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("out_role({}, {:?}):", setting.server_index, state.shared.read().unwrap().role);
        }
    }

    fn on_receive(&self, message: &message::Message, address: &SocketAddr) -> (bool, Option<entity::Role>) {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("on_receive({}, {:?}):", setting.server_index, state.shared.read().unwrap().role);
        }
        (true, None)
    }
    fn process(&self) -> (bool, Option<entity::Role>) {
        {
            let state = self.get_state();
            let setting = self.get_setting();
            debug!("process({}, {:?}):", setting.server_index, state.shared.read().unwrap().role);
        }
        // ？？？ TODO: アイドル状態の時にelection_timeoutを防ぐために
        // TODO: クライアントからコマンドを受け取ったら、entryをローカルに追加し
        //       ステートマシンにエントリを適用した後、応答する
        // TODO: Followerのlast log index >= nextIndexならばnextIndexから
        //       AppendEntriesを送信する
        //       成功ならばnextIndexとmatchIndexを更新する
        //       失敗ならば、nextIndexを減らしてリトライする
        // TODO: 半数以上が、N > commitIndex && matchIndex[i] >= N && term == currentTermならば
        //       commitIndex = N
        (true, None)
    }
}

// 他のノードからメッセージを受信するスレッド
pub fn receive_thread(my_index: usize, state: Arc<entity::State>, setting: Arc<entity::Setting>) -> () {
    fn send(my_index: usize, state: &entity::State, message: &message::Message) -> () {
        for (i, &(ref sender, ref receiver)) in state.channels.iter().enumerate() {
            // 自分には送らない
            if i != my_index {
                debug!("internal send({}): ->{}, {:?}", my_index, i, message);
                sender.send(message.clone()).unwrap();
            }
        }
    }

    debug!("receive_thread起動({}): {:?}, {:?}", my_index, state, setting);
    {
        let mut socket;
        {
            // ソケット初期化
            let serverSetting = &setting.servers[setting.server_index];
            let address = SocketAddrV4::new(Ipv4Addr::from_str(&serverSetting.0).unwrap(), serverSetting.1);
            socket = match UdpSocket::bind(address) {
                Ok(s) => s,
                Err(e) => panic!("couldn't bind socket: {}", e),
            };
            socket.set_read_timeout(Some(setting.read_timeout))
                .expect("failed to set_read_timeout");
        }
        // 起動時にFollowerになる
        let node = &RaftNode::new(&state, &setting);
        {
            // 処理ループ
            let wait = setting.receive_thread_loop_wait;
            let mut buf = [0; 2048];
            while !state.b_finish.load(Ordering::Relaxed) {
                let role;
                {
                    role = state.shared.read().unwrap().role;
                }
                {
                    // UDP受信
                    match socket.recv_from(&mut buf) {
                        Ok((size, address)) => {
                            if size > 0 {
                                Some((size, buf, address))
                            } else {
                                debug!("recv_from: size == 0");
                                None
                            }
                        },
                        Err(e) => {
                            match e.raw_os_error() {
                                Some(10060) => {
                                    // サーバごとの処理
                                    debug!("socket error: 10060");
                                    let mut node2 = node.borrow_mut();
                                    node2.process_for_all().and_then(|_|
                                        node2.process()
                                    );
                                    None
                                },
                                _ => panic!("couldn't bind socket: {}", e),
                            }
                        },
                    }.and_then(|(size, buf, address)| {
                        // デコード
                        let message: message::Message = bincode::rustc_serialize::decode(&buf).unwrap();
                        info!("recv UDP({}): {:?}", my_index, message);
                        // メッセージ受信処理
                        {
                            let mut node2 = node.borrow_mut();
                            node2.on_receive_for_all(&message, &address).and_then(|_|
                                node2.on_receive(&message, &address)
                            )
                        }}
                    ).and_then(|_| {
                        // サーバごとの処理
                        let mut node2 = node.borrow_mut();
                        node2.process_for_all().and_then(|_|
                            node2.process()
                        )}
                    )
                };
                // TODO: Thread.yield()にする
                thread::sleep(wait);
            }
        }
    }
    debug!("receive_thread終了({}): {:?}, {:?}", my_index, state, setting);
}
