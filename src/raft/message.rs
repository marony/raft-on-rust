use rustc_serialize;

#[derive(Debug, Clone, RustcDecodable, RustcEncodable)]
pub enum Message {
    Test,
    // term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit
    AppendEntries(u32, u16, u64, u32, Vec<u8>, u64),
    // term, success
    AppendEntriesReply(u32, bool),
    // term, candidateId, lastLogIndex, lastLogTerm
    RequestVote(u32, u16, u64, u32),
    // term, voteGranted
    RequestVoteReply(u32, bool),
}

unsafe impl Send for Message {}
unsafe impl Sync for Message {}
