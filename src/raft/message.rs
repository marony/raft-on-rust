use rustc_serialize;

#[derive(Debug, Clone, RustcDecodable, RustcEncodable)]
pub enum Message {
    Test,
    // term, leader_id, prev_log_index, prev_log_term, entries, leader_commit
    AppendEntries(u32, u16, u64, u32, Vec<u8>, u64),
    // term, success
    AppendEntriesReply(u32, bool),
    // term, candidate_id, last_log_index, last_log_term
    RequestVote(u32, u16, u64, u32),
    // term, voteGranted
    RequestVoteReply(u32, bool),
}

unsafe impl Send for Message {}
unsafe impl Sync for Message {}
