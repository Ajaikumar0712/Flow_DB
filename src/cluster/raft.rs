//! Raft — Leader election and log replication for FlowDB cluster.
//!
//! Implements the core Raft consensus algorithm described in the paper
//! "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout, 2014).
//!
//! # What is implemented
//! * Leader election with randomised election timeouts
//! * Heartbeat (empty AppendEntries) from leader to followers
//! * Log replication: leader appends entries, followers acknowledge
//! * Commit index advancement once a majority has replicated an entry
//! * Term-based safety: stale leaders step down when they see a higher term
//! * Volatile in-memory log (persistence delegated to WAL in production)
//!
//! # What is NOT implemented (out of scope for this prototype)
//! * Snapshotting / log compaction
//! * Cluster membership changes (joint consensus)
//! * Actual TCP transport — entries are dispatched via mpsc channels
//!   (callers must wire real network I/O on top)
//!
//! # Usage
//! ```rust,ignore
//! let node = RaftNode::new(NodeId(1), vec![NodeId(2), NodeId(3)]);
//! let (cmd_tx, resp_rx) = node.start(); // spawns background task
//! cmd_tx.send(RaftCommand::Propose { data: b"PUT x 1".to_vec() }).unwrap();
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ── Node ID ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

// ── Log entry ─────────────────────────────────────────────────────────────

/// A single entry in the Raft log.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Raft term when this entry was created.
    pub term: u64,
    /// Monotonically increasing log index (1-based).
    pub index: u64,
    /// Opaque command to be applied to the state machine.
    pub data: Vec<u8>,
}

// ── Role ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

// ── RPC messages ─────────────────────────────────────────────────────────

/// RequestVote RPC — sent by Candidates.
#[derive(Debug, Clone)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// RequestVote response.
#[derive(Debug, Clone)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

/// AppendEntries RPC — sent by Leaders (also used as heartbeat when entries is empty).
#[derive(Debug, Clone)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

/// AppendEntries response.
#[derive(Debug, Clone)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
    /// Hint: the follower's next expected index (for fast backtracking).
    pub match_index: u64,
}

// ── Commands into the Raft node ───────────────────────────────────────────

pub enum RaftCommand {
    /// Propose a new log entry to be replicated (only leader processes this).
    Propose { data: Vec<u8> },
    /// Feed an incoming RequestVote RPC.
    HandleRequestVote { req: RequestVote, reply_tx: std::sync::mpsc::Sender<RequestVoteReply> },
    /// Feed an incoming AppendEntries RPC.
    HandleAppendEntries { req: AppendEntries, reply_tx: std::sync::mpsc::Sender<AppendEntriesReply> },
    /// Feed a RequestVote reply from a peer.
    RecvRequestVoteReply { from: NodeId, reply: RequestVoteReply },
    /// Feed an AppendEntries reply from a peer.
    RecvAppendEntriesReply { from: NodeId, reply: AppendEntriesReply },
}

// ── Node state ────────────────────────────────────────────────────────────

/// Persistent + volatile Raft state for one server.
pub struct RaftState {
    // ── Persistent (should survive restart) ──────────────────────────────
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>, // index 0 is a sentinel (term=0, index=0)

    // ── Volatile ─────────────────────────────────────────────────────────
    pub commit_index: u64,
    pub last_applied: u64,
    pub role: Role,

    // ── Leader volatile ──────────────────────────────────────────────────
    /// next_index[peer] = next log index to send to that peer.
    pub next_index: HashMap<NodeId, u64>,
    /// match_index[peer] = highest log index known to be replicated on that peer.
    pub match_index: HashMap<NodeId, u64>,

    // ── Election timeout ─────────────────────────────────────────────────
    pub last_heartbeat: Instant,
    pub election_timeout: Duration,

    // ── Cluster membership ───────────────────────────────────────────────
    pub id: NodeId,
    pub peers: Vec<NodeId>,
}

impl RaftState {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        let election_timeout = Self::random_timeout(id.0);
        Self {
            current_term: 0,
            voted_for: None,
            log: vec![LogEntry { term: 0, index: 0, data: Vec::new() }], // sentinel
            commit_index: 0,
            last_applied: 0,
            role: Role::Follower,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            last_heartbeat: Instant::now(),
            election_timeout,
            id,
            peers,
        }
    }

    fn random_timeout(seed: u64) -> Duration {
        // Randomised timeout in [150ms, 300ms] — avoids split votes.
        const A: u64 = 6_364_136_223_846_793_005;
        const C: u64 = 1_442_695_040_888_963_407;
        let x = A.wrapping_mul(seed.wrapping_add(1)).wrapping_add(C);
        let ms = 150 + (x >> 32) % 150;
        Duration::from_millis(ms)
    }

    pub fn last_log_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    /// True if `(last_log_term, last_log_index)` is at least as up-to-date as ours.
    pub fn is_log_up_to_date(&self, term: u64, index: u64) -> bool {
        term > self.last_log_term()
            || (term == self.last_log_term() && index >= self.last_log_index())
    }

    /// Step down to follower if we see a higher term.  Returns true if we stepped down.
    pub fn maybe_step_down(&mut self, term: u64) -> bool {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for   = None;
            self.role        = Role::Follower;
            self.last_heartbeat = Instant::now();
            true
        } else {
            false
        }
    }

    /// Start an election: become Candidate, increment term, vote for self.
    /// Returns the RequestVote payload to broadcast to peers.
    pub fn start_election(&mut self) -> RequestVote {
        self.current_term += 1;
        self.role         = Role::Candidate;
        self.voted_for    = Some(self.id);
        self.election_timeout = Self::random_timeout(self.id.0 ^ self.current_term);
        self.last_heartbeat   = Instant::now();
        RequestVote {
            term:            self.current_term,
            candidate_id:    self.id,
            last_log_index:  self.last_log_index(),
            last_log_term:   self.last_log_term(),
        }
    }

    /// Become leader: initialise next_index / match_index for every peer.
    pub fn become_leader(&mut self) {
        let next = self.last_log_index() + 1;
        self.role = Role::Leader;
        for &peer in &self.peers {
            self.next_index.insert(peer, next);
            self.match_index.insert(peer, 0);
        }
    }

    // ── AppendEntries handler ─────────────────────────────────────────────

    pub fn handle_append_entries(&mut self, req: AppendEntries) -> AppendEntriesReply {
        // 1. Reply false if term < currentTerm.
        if req.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term, success: false, match_index: 0,
            };
        }
        self.maybe_step_down(req.term);
        self.last_heartbeat = Instant::now();

        // 2. Reply false if log doesn't contain an entry at prevLogIndex
        //    whose term matches prevLogTerm.
        let prev_ok = req.prev_log_index == 0
            || self.log.get(req.prev_log_index as usize)
                .map(|e| e.term == req.prev_log_term)
                .unwrap_or(false);

        if !prev_ok {
            return AppendEntriesReply {
                term: self.current_term, success: false, match_index: 0,
            };
        }

        // 3. Truncate conflicting suffix and append new entries.
        for entry in req.entries {
            let idx = entry.index as usize;
            if idx < self.log.len() {
                if self.log[idx].term != entry.term {
                    self.log.truncate(idx);
                    self.log.push(entry);
                }
                // else already have it — skip.
            } else {
                self.log.push(entry);
            }
        }

        // 4. Advance commit index.
        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.last_log_index());
        }

        AppendEntriesReply {
            term: self.current_term,
            success: true,
            match_index: self.last_log_index(),
        }
    }

    // ── RequestVote handler ───────────────────────────────────────────────

    pub fn handle_request_vote(&mut self, req: RequestVote) -> RequestVoteReply {
        if req.term < self.current_term {
            return RequestVoteReply { term: self.current_term, vote_granted: false };
        }
        self.maybe_step_down(req.term);

        let can_vote = self.voted_for.is_none()
            || self.voted_for == Some(req.candidate_id);
        let log_ok = self.is_log_up_to_date(req.last_log_term, req.last_log_index);

        if can_vote && log_ok {
            self.voted_for = Some(req.candidate_id);
            self.last_heartbeat = Instant::now(); // reset timeout on granting vote
            RequestVoteReply { term: self.current_term, vote_granted: true }
        } else {
            RequestVoteReply { term: self.current_term, vote_granted: false }
        }
    }

    // ── Leader: advance commit index ─────────────────────────────────────

    /// Check if a new commit index can be established (majority has replicated).
    pub fn advance_commit_index(&mut self) {
        if self.role != Role::Leader { return; }
        let n_peers = self.peers.len();
        let majority = (n_peers + 1) / 2 + 1; // includes the leader itself

        // Walk from the highest possible N down to commit_index+1.
        let max_n = self.last_log_index();
        for n in (self.commit_index + 1..=max_n).rev() {
            // Count peers that have replicated up to N.
            let replicated = 1 + self.match_index.values().filter(|&&m| m >= n).count();
            if replicated >= majority {
                // Only commit entries from the current term (Raft safety rule).
                if let Some(entry) = self.log.get(n as usize) {
                    if entry.term == self.current_term {
                        self.commit_index = n;
                        break;
                    }
                }
            }
        }
    }
}

// ── RaftNode ─────────────────────────────────────────────────────────────

/// Thread-safe Raft node.
pub struct RaftNode {
    pub state: Arc<Mutex<RaftState>>,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            state: Arc::new(Mutex::new(RaftState::new(id, peers))),
        }
    }

    /// Tick the node's election timer — call periodically (e.g. every 10ms).
    /// Returns a `RequestVote` if an election should be started.
    pub fn tick(&self) -> Option<RequestVote> {
        let mut s = self.state.lock().unwrap();
        if s.role != Role::Leader && s.last_heartbeat.elapsed() > s.election_timeout {
            Some(s.start_election())
        } else {
            None
        }
    }

    /// Receive a `RequestVote` RPC.
    pub fn on_request_vote(&self, req: RequestVote) -> RequestVoteReply {
        self.state.lock().unwrap().handle_request_vote(req)
    }

    /// Receive a `RequestVoteReply`.
    /// Returns `AppendEntries` broadcast payloads if this node just became leader.
    pub fn on_request_vote_reply(
        &self,
        _from: NodeId,
        reply: RequestVoteReply,
        votes_received: &mut usize,
    ) -> Vec<(NodeId, AppendEntries)> {
        let mut s = self.state.lock().unwrap();
        if s.maybe_step_down(reply.term) { return Vec::new(); }
        if s.role != Role::Candidate || !reply.vote_granted { return Vec::new(); }

        *votes_received += 1;
        let majority = (s.peers.len() + 1) / 2 + 1;
        if *votes_received >= majority {
            s.become_leader();
            // Send initial heartbeats to assert leadership.
            let entries: Vec<(NodeId, AppendEntries)> = s.peers.iter().map(|&peer| {
                let ae = AppendEntries {
                    term: s.current_term,
                    leader_id: s.id,
                    prev_log_index: s.last_log_index(),
                    prev_log_term: s.last_log_term(),
                    entries: Vec::new(), // heartbeat
                    leader_commit: s.commit_index,
                };
                (peer, ae)
            }).collect();
            return entries;
        }
        Vec::new()
    }

    /// Receive an `AppendEntries` RPC.
    pub fn on_append_entries(&self, req: AppendEntries) -> AppendEntriesReply {
        self.state.lock().unwrap().handle_append_entries(req)
    }

    /// Receive an `AppendEntriesReply`.
    pub fn on_append_entries_reply(&self, from: NodeId, reply: AppendEntriesReply) {
        let mut s = self.state.lock().unwrap();
        if s.maybe_step_down(reply.term) { return; }
        if reply.success {
            s.match_index.insert(from, reply.match_index);
            s.next_index.insert(from, reply.match_index + 1);
            s.advance_commit_index();
        } else {
            // Decrement next_index to try with an earlier entry.
            let ni = s.next_index.get(&from).copied().unwrap_or(1);
            s.next_index.insert(from, ni.saturating_sub(1).max(1));
        }
    }

    /// Propose a new command (leader only).
    /// Returns `Ok(log_index)` if queued, or `Err` if not leader.
    pub fn propose(&self, data: Vec<u8>) -> Result<u64, &'static str> {
        let mut s = self.state.lock().unwrap();
        if s.role != Role::Leader {
            return Err("not leader");
        }
        let index = s.last_log_index() + 1;
        let term  = s.current_term;
        s.log.push(LogEntry { term, index, data });
        Ok(index)
    }

    pub fn is_leader(&self) -> bool {
        self.state.lock().unwrap().role == Role::Leader
    }

    pub fn current_term(&self) -> u64 {
        self.state.lock().unwrap().current_term
    }

    pub fn commit_index(&self) -> u64 {
        self.state.lock().unwrap().commit_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn three_node_cluster() -> (RaftNode, RaftNode, RaftNode) {
        let n1 = RaftNode::new(NodeId(1), vec![NodeId(2), NodeId(3)]);
        let n2 = RaftNode::new(NodeId(2), vec![NodeId(1), NodeId(3)]);
        let n3 = RaftNode::new(NodeId(3), vec![NodeId(1), NodeId(2)]);
        (n1, n2, n3)
    }

    #[test]
    fn test_initial_state_is_follower() {
        let (n1, _, _) = three_node_cluster();
        assert_eq!(n1.state.lock().unwrap().role, Role::Follower);
    }

    #[test]
    fn test_election_and_leader_emergence() {
        let (n1, n2, n3) = three_node_cluster();

        // Force n1 to time out and start an election.
        {
            let mut s = n1.state.lock().unwrap();
            s.last_heartbeat = Instant::now() - Duration::from_secs(10);
        }
        let vote_req = n1.tick().expect("n1 should start election");
        assert_eq!(n1.state.lock().unwrap().role, Role::Candidate);

        // n2 and n3 grant their votes.
        let r2 = n2.on_request_vote(vote_req.clone());
        let r3 = n3.on_request_vote(vote_req.clone());
        assert!(r2.vote_granted);
        assert!(r3.vote_granted);

        let mut votes = 1; // n1 voted for itself
        n1.on_request_vote_reply(NodeId(2), r2, &mut votes);
        n1.on_request_vote_reply(NodeId(3), r3, &mut votes);

        assert!(n1.is_leader(), "n1 should be leader after receiving majority votes");
    }

    #[test]
    fn test_log_replication() {
        let (n1, n2, _n3) = three_node_cluster();
        // Directly make n1 the leader.
        n1.state.lock().unwrap().become_leader();
        n1.state.lock().unwrap().current_term = 1;

        let idx = n1.propose(b"SET x 42".to_vec()).unwrap();
        assert_eq!(idx, 1);

        // Build AppendEntries for n2.
        let ae = {
            let s = n1.state.lock().unwrap();
            AppendEntries {
                term: s.current_term,
                leader_id: s.id,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: s.log[1..].to_vec(),
                leader_commit: 0,
            }
        };
        let reply = n2.on_append_entries(ae);
        assert!(reply.success);
        assert_eq!(reply.match_index, 1);

        // Leader processes the reply.
        n1.on_append_entries_reply(NodeId(2), reply);
        // n1 needs at least one peer to match (majority = 2 in a 3-node cluster).
        // With n2 replicated, commit index should advance.
        assert_eq!(n1.commit_index(), 1, "leader should commit once majority replicates");
    }

    #[test]
    fn test_stale_leader_steps_down() {
        let n1 = RaftNode::new(NodeId(1), vec![NodeId(2)]);
        n1.state.lock().unwrap().become_leader();
        n1.state.lock().unwrap().current_term = 1;

        // Receive a message from a higher-term node.
        let reply = AppendEntriesReply { term: 5, success: false, match_index: 0 };
        n1.on_append_entries_reply(NodeId(2), reply);
        assert_eq!(n1.state.lock().unwrap().role, Role::Follower, "stale leader should step down");
        assert_eq!(n1.current_term(), 5);
    }
}
