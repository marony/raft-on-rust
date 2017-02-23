use std::env;
use std::{thread, time};
use std::sync::{Arc, RwLock};
#[macro_use] extern crate log;
extern crate env_logger;
extern crate bincode;
extern crate rustc_serialize;

mod raft;

// RUST_BACKTRACE=1 RUST_LOG=raft_rust=debug cargo run 1000 0 127.0.0.1:8080 127.0.0.1:8081 127.0.0.1:8082

// ノード分のプロセスを起動する
// 自分がどのノードかインデックスで指定する
fn help() -> () {
    println!("program message_count my_index server0:port0 server1:port1 server2:port2 …");
}

fn main() -> () {
    env_logger::init().unwrap();
    let args: Vec<String> = env::args().collect();
    match args.len() {
        1 | 2 | 3 => {
            // 引数が足りない
            help();
            return;
        }
        _ => {
            // メッセージ数
            let message_count: i32 = match args[1].parse() {
                Ok(n) => n,
                Err(_) => {
                    error!("invalid argument: {}", args[1]);
                    help();
                    return;
                }
            };
            // 自分のサーバ設定のインデックス(0-)
            let my_index: usize = match args[2].parse() {
                Ok(n) => n,
                Err(_) => {
                    error!("invalid argument: {}", args[2]);
                    help();
                    return;
                }
            };
            // サーバ設定
            let servers: Vec<(String, u16)> = args.iter().skip(3).map(|s| {
                let ss = s.split(':').collect::<Vec<_>>();
                let host = ss[0];
                let port: u16 = match ss[1].parse() {
                    Ok(n) => n,
                    Err(_) => 0
                };
                (host.to_string(), port)
            }).collect::<Vec<_>>();

            // 引数表示
            info!("message_count = {}, my_index = {}", message_count, my_index);
            for (i, s) in servers.iter().enumerate() {
                match s {
                    &(ref h, p) => {
                        if i == my_index {
                            info!(" -> ")
                        } else {
                            info!("    ")
                        }
                        info!("{}: {}, {}", i, h, p)
                    }
                }
            }

            // 実行開始
            raft::process(message_count, my_index, &servers);
        }
    }
}
