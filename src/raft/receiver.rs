#![feature(conservative_impl_trait)]

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

trait RaftNode : Debug {
    fn get_role(&self) -> entity::Role;
    fn get_state(&self) -> Arc<entity::State>;
    fn get_setting(&self) -> Arc<entity::Setting>;

    fn to_role(&self, role: &entity::Role) -> Box<RaftNode> {
        // FIXME: #![feature(conservative_impl_trait)]が効かない
        info!("{:?} -> {:?}", self.get_role(), role);
        self.out_role(role);
        let state = self.get_state().clone();
        let setting = self.get_setting().clone();
        {
            let mut shared = state.shared.write().unwrap();
            shared.role = *role;
        }
        let n: Box<RaftNode> = match *role {
            entity::Role::Follower =>
                Box::new(Follower::new(&state, &setting)),
            entity::Role::Candidate =>
                Box::new(Candidate::new(&state, &setting)),
            entity::Role::Leader =>
                Box::new(Leader::new(&state, &setting)),
        };
        n.on_role(role);
        n
    }
    fn on_role(&self, from_role: &entity::Role) -> ();
    fn out_role(&self, to_role: &entity::Role) -> ();

    fn on_receive_for_all(&self, message: &message::Message, &address: &SocketAddr) -> Option<()> {
        debug!("on_receive_for_all({}):", self.get_setting().server_index);
        Some(())
    }
    fn on_receive(&self, message: &message::Message, &address: &SocketAddr) -> Option<()>;
    fn process_for_all(&self) -> Option<()> {
        debug!("process_for_all({}):", self.get_setting().server_index);
        // TODO: commitIndex, lastAppliedのチェック
        // TODO: 受信時 term, currentTermのチェック
        Some(())
    }
    fn process(&self) -> Option<()>;
}

#[derive(Debug)]
struct Follower {
    state: Arc<entity::State>,
    setting: Arc<entity::Setting>,
}

impl Follower {
    pub fn new(state: &Arc<entity::State>, setting: &Arc<entity::Setting>) -> Follower {
        {
            // FIXME: 場所がよくない
            // 受信時刻を更新
            let mut shared = state.shared.write().unwrap();
            shared.receive_time = time::Instant::now();
        }
        Follower { state: state.clone(), setting: setting.clone() }
    }
}

impl RaftNode for Follower {
    fn get_role(&self) -> entity::Role {
        entity::Role::Follower
    }
    fn get_state(&self) -> Arc<entity::State> {
        self.state.clone()
    }
    fn get_setting(&self) -> Arc<entity::Setting> {
        self.setting.clone()
    }

    fn on_role(&self, from_role: &entity::Role) -> () {
        debug!("on_role({}):", self.setting.server_index);
        // 受信時刻を更新
        let mut shared = self.state.shared.write().unwrap();
        shared.receive_time = time::Instant::now();
    }
    fn out_role(&self, to_role: &entity::Role) -> () {
        debug!("out_role({}):", self.setting.server_index);
    }

    fn on_receive(&self, message: &message::Message, &address: &SocketAddr) -> Option<()> {
        debug!("on_receive({}):", self.setting.server_index);
        Some(())
    }
    fn process(&self) -> Option<()> {
        debug!("process({}):", self.setting.server_index);
        // TODO: AppendEntriesに返事をする
        // TODO: AppendEntriesか選挙がタイムアウトしたらCandidateになる
        Some(())
    }
}

#[derive(Debug)]
struct Candidate {
    state: Arc<entity::State>,
    setting: Arc<entity::Setting>,
}

impl Candidate {
    pub fn new(state: &Arc<entity::State>, setting: &Arc<entity::Setting>) -> Candidate {
        Candidate { state: state.clone(), setting: setting.clone() }
    }
}

impl RaftNode for Candidate{
    fn get_role(&self) -> entity::Role {
        entity::Role::Candidate
    }
    fn get_state(&self) -> Arc<entity::State> {
        self.state.clone()
    }
    fn get_setting(&self) -> Arc<entity::Setting> {
        self.setting.clone()
    }

    fn on_role(&self, from_role: &entity::Role) -> () {
        debug!("on_role({}):", self.setting.server_index);
        // TODO: currentTerm更新
        // TODO: 自分に投票
        // TODO: election_timeoutをリセット
        // TODO: RequestVote RPCを他のノードに送信
    }
    fn out_role(&self, to_role: &entity::Role) -> () {
        debug!("out_role({}):", self.setting.server_index);
    }

    fn on_receive(&self, message: &message::Message, &address: &SocketAddr) -> Option<()> {
        debug!("on_receive({}):", self.setting.server_index);
        Some(())
    }
    fn process(&self) -> Option<()> {
        debug!("process({}):", self.setting.server_index);
        // TODO: マジョリティから投票を受け取ったらLeaderになる
        // TODO: 新しいリーダからAppendEntriesを受け取ったらFollowerになる
        // TODO: election_timeoutしたら新規に選挙を始める
        Some(())
    }
}

#[derive(Debug)]
struct Leader {
    state: Arc<entity::State>,
    setting: Arc<entity::Setting>,
}

impl Leader {
    pub fn new(state: &Arc<entity::State>, setting: &Arc<entity::Setting>) -> Leader {
        Leader { state: state.clone(), setting: setting.clone() }
    }
}

impl RaftNode for Leader {
    fn get_role(&self) -> entity::Role {
        entity::Role::Leader
    }
    fn get_state(&self) -> Arc<entity::State> {
        self.state.clone()
    }
    fn get_setting(&self) -> Arc<entity::Setting> {
        self.setting.clone()
    }

    fn on_role(&self, from_role: &entity::Role) -> () {
        debug!("on_role({}):", self.setting.server_index);
        // TODO: 選挙が終わったら最初の空のAppendEntriesを送信する
    }
    fn out_role(&self, to_role: &entity::Role) -> () {
        debug!("out_role({}):", self.setting.server_index);
    }

    fn on_receive(&self, message: &message::Message, &address: &SocketAddr) -> Option<()> {
        debug!("on_receive({}):", self.setting.server_index);
        Some(())
    }
    fn process(&self) -> Option<()> {
        debug!("process({}):", self.setting.server_index);
        // ？？？ TODO: アイドル状態の時にelection_timeoutを防ぐために
        // TODO: クライアントからコマンドを受け取ったら、entryをローカルに追加し
        //       ステートマシンにエントリを適用した後、応答する
        // TODO: Followerのlast log index >= nextIndexならばnextIndexから
        //       AppendEntriesを送信する
        //       成功ならばnextIndexとmatchIndexを更新する
        //       失敗ならば、nextIndexを減らしてリトライする
        // TODO: 半数以上が、N > commitIndex && matchIndex[i] >= N && term == currentTermならば
        //       commitIndex = N
        Some(())
    }
}

// 他のノードからメッセージを受信するスレッド
pub fn receive_thread(my_index: usize, state: Arc<entity::State>, setting: Arc<entity::Setting>) -> () {
    fn send(my_index: usize, state: &entity::State, message: &message::Message) -> () {
        for (i, &(ref sender, ref receiver)) in state.channels.iter().enumerate() {
            // 自分には送らない
            if i != my_index {
                debug!("send({}): ->{}, {:?}", my_index, i, message);
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
        let mut node: Box<RaftNode> = Box::new(Follower::new(&state, &setting));
        {
            // 処理ループ
            let wait = setting.receive_thread_loop_wait;
            let mut buf = [0; 2048];
            while !state.b_finish.load(Ordering::Relaxed) {
                let role = node.get_role();
                {
                    // UDP受信
                    match socket.recv_from(&mut buf) {
                        Ok((size, address)) => {
                            if size > 0 {
                                Some((size, buf, address))
                            } else {
                                None
                            }
                        },
                        Err(e) => panic!("couldn't bind socket: {}", e),
                    }.and_then(|(size, buf, address)| {
                        // デコード
                        let message: message::Message = bincode::rustc_serialize::decode(&buf).unwrap();
                        info!("recv UDP({}): {:?}", my_index, message);
                        // メッセージ受信処理
                        node.on_receive_for_all(&message, &address).and_then(|_|
                            node.on_receive(&message, &address)
                        )}
                    ).and_then(|_|
                        // サーバごとの処理
                        node.process_for_all().and_then(|_|
                            node.process()
                        )
                    )
                };
                // FIXME: デバッグコード
                //        自分に適当なメッセージを送る
                send(my_index, &state, &message::Message::Test);
                // TODO: Thread.yield()にする
                thread::sleep(wait);
            }
        }
    }
    debug!("receive_thread終了({}): {:?}, {:?}", my_index, state, setting);
}
