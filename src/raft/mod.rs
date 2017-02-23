use std::sync::{Arc, RwLock};
use std::{thread, time};
use std::sync::atomic::Ordering;

mod entity;
mod message;
mod receiver;
mod sender;

pub fn process(message_count: i32, my_index: usize, servers: &Vec<(String, u16)>) -> () {
    let setting = Arc::new(entity::Setting::new(message_count, my_index, servers));
    // TODO: ディスクから読み込み
    let persistent = Arc::new(RwLock::new(entity::PersistentState::new()));
    let shared = Arc::new(RwLock::new(entity::ShareState::new()));
    let state = Arc::new(entity::State::new(servers.len(), &persistent, &shared));

    let rt = create_receive_thread(message_count, my_index, servers, &setting, &state);
    let sts = create_send_threads(message_count, my_index, servers, &setting, &state);

    {
        info!("waiting");
        thread::sleep(time::Duration::from_millis(10000));
        info!("waited");
        state.b_finish.store(true, Ordering::Relaxed);
        info!("stopping: {}", state.b_finish.load(Ordering::Relaxed));
    }

    // 受信スレッド終了待ち
    rt.join().unwrap();
    // 送信スレッド終了待ち
    for st in sts.into_iter() {
        st.join().unwrap();
    }
}

// 受信スレッド起動
fn create_receive_thread(message_count: i32, my_index: usize, servers: &Vec<(String, u16)>,
                         setting: &Arc<entity::Setting>, state: &Arc<entity::State>) -> thread::JoinHandle<()> {
    let _setting = setting.clone();
    let _state = state.clone();
    let rt = thread::spawn(move || receiver::receive_thread(my_index, _state, _setting));
    rt
}

// 送信スレッド起動
fn create_send_threads(message_count: i32, my_index: usize, servers: &Vec<(String, u16)>,
                       setting: &Arc<entity::Setting>, state: &Arc<entity::State>) -> Vec<thread::JoinHandle<()>> {
    let sts: Vec<thread::JoinHandle<()>> = servers.iter().enumerate().
        filter(|is: &(usize, &(String, u16))| is.0 != my_index).
        map(|is: (usize, &(String, u16))| {
        let _setting = setting.clone();
        let _state = state.clone();
        let iss = is.0;
        thread::spawn(move || sender::send_thread(iss, _state, _setting))
    }).collect::<Vec<_>>();
    sts
}
