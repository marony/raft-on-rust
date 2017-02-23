use rustc_serialize;

#[derive(Debug, Copy, Clone, RustcDecodable, RustcEncodable)]
pub enum Message {
    Test,
    // TODO: entries[]をCopyできる配列にしないと！！！
    // term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit
    AppendEntries(u32, u16, u64, u32, u8, u64),
    // term, success
    AppendEntriesReply(u32, bool),
    // term, candidateId, lastLogIndex, lastLogTerm
    RequestVote(u32, u16, u64, u32),
    // term, voteGranted
    RequestVoteReply(u32, bool),
}

unsafe impl Send for Message {}
unsafe impl Sync for Message {}
