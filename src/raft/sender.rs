use std::sync::{Arc, RwLock};
use std::{thread, time};
use std::sync::atomic::Ordering;
use std::sync::mpsc::TryRecvError;
use std::net::{SocketAddrV4, Ipv4Addr};
use std::net::UdpSocket;
use std::str::FromStr;

extern crate rustc_serialize;
extern crate bincode;

use super::entity as entity;
use super::message as message;

// 他のノードにメッセージを送信するスレッド(自分への送信スレッドは存在しない)
pub fn send_thread(server_index: usize, state: Arc<entity::State>, setting: Arc<entity::Setting>) -> () {
    fn recv(server_index: usize, state: &entity::State) -> Result<message::Message, TryRecvError> {
        state.channels[server_index].1.try_recv()
    }

    debug!("send_thread起動({}): {:?}, {:?}", server_index, state, setting);
    {
        let mut socket;
        let server_setting = &setting.servers[server_index];
        let send_address = SocketAddrV4::new(Ipv4Addr::from_str(&server_setting.0).unwrap(), server_setting.1);
        {
            // ソケット初期化
            socket = match UdpSocket::bind("0.0.0.0:0") {
                Ok(s) => s,
                Err(e) => panic!("couldn't bind socket: {}", e),
            };
        }
        {
            // 処理ループ
            let wait = setting.send_thread_loop_wait;
            while !state.b_finish.load(Ordering::Relaxed) {
                if let Ok(message) = recv(server_index, &state) {
                    // UDP送信
                    debug!("internal recv({}): {:?}", server_index, message);
                    info!("send UDP({}): {}, {:?}", server_index, send_address, message);
                    let encoded = bincode::rustc_serialize::encode(&message, bincode::SizeLimit::Infinite).unwrap();
                    socket.send_to(&encoded, send_address);
                }
                {
                    // FIXME: デバッグコード
                    //        自分に適当なUDPパケットを送る
                    let my_setting = &setting.servers[setting.server_index];
                    let my_address = SocketAddrV4::new(Ipv4Addr::from_str(&my_setting.0).unwrap(), my_setting.1);
                    let message = message::Message::Test;
                    debug!("send UDP({}): {}, {:?}", setting.server_index, my_address, message);
                    let encoded = bincode::rustc_serialize::encode(&message, bincode::SizeLimit::Infinite).unwrap();
                    socket.send_to(&encoded, my_address);
                }
                thread::sleep(wait);
            }
        }
    }
    debug!("send_thread終了({}): {:?}, {:?}", server_index, state, setting);
}
