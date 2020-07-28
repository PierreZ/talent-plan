use std::sync::mpsc::{channel, Sender, TryRecvError};
use std::sync::{Arc, Mutex};

use futures::channel::mpsc::UnboundedSender;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use rand::Rng;
use std::time::Duration;
use std::{thread, time};

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
    pub is_candidate: bool,
    pub is_follower: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
    /// Whether this peer believes it is a candidate.
    pub fn is_candidate(&self) -> bool {
        self.is_candidate
    }

    pub fn set_as_leader(&mut self) {
        self.is_leader = true;
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    // apply_ch is a channel on which the tester or service
    // expects Raft to send ApplyMsg messages.
    pub apply_ch: UnboundedSender<ApplyMsg>,

    voted_for: Option<u64>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
            //  voted_for: None,
            apply_ch,
            voted_for: None,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf
    }

    pub fn set_state(&mut self, term: u64, is_leader: bool, is_candidate: bool, is_follower: bool) {
        let leader = State {
            term,
            is_leader,
            is_candidate,
            is_follower,
        };
        self.state = Arc::new(leader);
    }

    pub fn get_vote_for(&self) -> Option<u64> {
        self.voted_for
    }
    pub fn set_vote_for(&mut self, vote: Option<u64>) {
        self.voted_for = vote;
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
        tx: Sender<Result<RequestVoteReply>>,
    ) {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            match tx.send(res) {
                Ok(_) => {}
                Err(err) => unimplemented!("cannot send message: {}", err.to_string()),
            }
        });
    }

    fn send_append_entry_request(
        &self,
        server: usize,
        args: AppendEntryRequest,
        tx: Sender<Result<AppendEntryResponse>>,
    ) {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            match tx.send(res) {
                Ok(_) => {}
                Err(err) => unimplemented!("cannot send message: {}", err.to_string()),
            }
        });
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Raft>>,
    election_timeout: time::Duration,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let election_timeout = rand::thread_rng().gen_range(150, 350);
        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
            election_timeout: Duration::from_millis(election_timeout),
        };

        Node::create_vote_thread(node.clone());
        node
    }

    pub fn create_vote_thread(node: Node) {
        let mut term = node.term();
        let mut iteration_without_passing_follower = 0;
        thread::spawn(move || loop {
            let peers;
            let me;
            {
                let mut guard = match node.raft.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };

                // reset state
                guard.set_state(term, false, false, false);

                peers = guard.peers.clone();
                me = guard.me;
            }

            let election_timeout = Duration::from_millis(rand::thread_rng().gen_range(300, 500));
            //let election_timeout = Duration::from_millis(((me * 5 * 100) + 100) as u64);
            let now = time::Instant::now();
            info!(
                "node{} at term={} will vote in {:#?}",
                me, term, election_timeout
            );

            thread::sleep(election_timeout);

            {
                let mut guard = match node.raft.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };

                if guard.state.is_follower {
                    info!("exiting voting thread because node{} is a follower", me);
                    return;
                }

                match guard.voted_for {
                    None => {}
                    Some(voted_for) => {
                        if iteration_without_passing_follower >= 1 {
                            info!("not an follower and too much iteration, resetting things");
                            iteration_without_passing_follower = 0;
                            guard.set_vote_for(None)
                        } else {
                            iteration_without_passing_follower += 1;
                            info!("node{} has already voted for {}, continuing", me, voted_for);
                        }
                        continue;
                    }
                }

                term += 1;
                guard.set_state(term, false, true, false);
                guard.set_vote_for(Some(me as u64));
            }

            info!(
                "node{} at term={} is voting for himself after {:#?}",
                me,
                term,
                now.elapsed()
            );

            let (tx, rx) = channel();
            for peer_number in 0..peers.len() {
                let tx_clone = tx.clone();
                if peer_number != me {
                    info!("node{} sending RequestVoteArgs to {}", me, peer_number);
                    let args = RequestVoteArgs {
                        candidate_id: me as u64,
                        term,
                    };

                    {
                        let guard = match node.raft.lock() {
                            Ok(guard) => guard,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        guard.send_request_vote(peer_number, args, tx_clone);
                    }
                }
            }

            // retrieve results
            let mut request_vote_results = vec![];
            while request_vote_results.len() != (peers.len() - 1) {
                match rx.try_recv() {
                    Ok(result) => {
                        request_vote_results.push(result);
                    }
                    Err(err) => match err {
                        TryRecvError::Empty => {}
                        TryRecvError::Disconnected => panic!("TryRecvError::Disconnected"),
                    },
                }
            }

            info!(
                "node{} received all results: {:?}",
                me,
                request_vote_results.clone()
            );
            // he voted for himself
            let mut voted_granted = 1;
            let mut ko = 0;
            let mut vote_not_granted = 0;
            let majority = 2;

            for result in request_vote_results {
                match result {
                    Ok(response) => {
                        if response.vote_granted {
                            voted_granted += 1
                        } else {
                            vote_not_granted += 1
                        }
                    }
                    Err(_) => ko += 1,
                }
            }

            if voted_granted >= majority {
                info!(
                    "we have a new leader {} with {}/{} at term {}",
                    me, voted_granted, majority, term
                );
                {
                    let mut guard = match node.raft.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    guard.set_state(term, true, false, false);
                }

                // broadcasting leader state
                let (tx, rx) = channel();
                for peer_number in 0..peers.len() {
                    let tx_clone = tx.clone();
                    if peer_number != me {
                        info!("node{} sending AppendEntry to {}", me, peer_number);
                        let args = AppendEntryRequest {
                            leader_id: me as u64,
                            term,
                        };

                        {
                            let guard = match node.raft.lock() {
                                Ok(guard) => guard,
                                Err(poisoned) => poisoned.into_inner(),
                            };
                            guard.send_append_entry_request(peer_number, args, tx_clone);
                        }
                    }
                }

                // retrieve results
                let mut append_entries_results = vec![];
                while append_entries_results.len() != (peers.len() - 1) {
                    match rx.try_recv() {
                        Ok(result) => {
                            append_entries_results.push(result);
                        }
                        Err(err) => match err {
                            TryRecvError::Empty => {}
                            TryRecvError::Disconnected => panic!("TryRecvError::Disconnected"),
                        },
                    }
                }

                dbg!(append_entries_results);

                return;
            } else {
                info!(
                    "not enough answers for {}, we have {} ko and {} ungranted votes for a majority of {}!",
                    me, ko, vote_not_granted, majority
                );
            }
        });
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.raft.lock().unwrap().state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.raft.lock().unwrap().state.is_leader
    }

    /// Whether this peer believes it is the leader.
    pub fn is_candidate(&self) -> bool {
        self.raft.lock().unwrap().state.is_candidate
    }

    /// Whether this peer believes it is the leader.
    pub fn is_follower(&self) -> bool {
        self.raft.lock().unwrap().state.is_follower
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
            is_candidate: self.is_candidate(),
            is_follower: self.is_follower(),
        }
    }

    pub fn get_vote_for(&self) -> Option<u64> {
        self.raft.lock().unwrap().get_vote_for()
    }

    pub fn get_me(&self) -> usize {
        self.raft.lock().unwrap().me
    }

    pub fn set_vote_for(&self, vote: Option<u64>) {
        let mut raft = self.raft.lock().unwrap();
        raft.set_vote_for(vote);
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, request: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        info!("inside request_vote rpc with req={:#?}", request);
        let mut result = RequestVoteReply {
            term: 0,
            vote_granted: false,
        };

        let me;
        {
            let mut guard = match self.raft.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };

            info!(
                "node{} has acquired his rpc lock at term {}",
                guard.me, guard.state.term
            );

            result.term = guard.state.term;
            let voted_for = guard.get_vote_for();
            me = guard.me;

            dbg!(me, request.term, result.term, guard.state.is_candidate);
            match voted_for {
                None => {
                    if result.term < request.term && !guard.state.is_candidate {
                        info!(
                            "node{} is voting for {}! current_term={}, request_term={}",
                            me, request.candidate_id, guard.state.term, request.term
                        );
                        result.vote_granted = true;
                        guard.set_vote_for(Some(request.candidate_id));
                        let is_candidate = guard.state.is_candidate;
                        let is_follower = guard.state.is_follower;
                        guard.set_state(request.term, false, is_candidate, is_follower);
                    }
                }
                Some(voted_for) => {
                    info!("node{} has voted for {}!", me, voted_for);
                }
            }
        }
        info!(
            "node{} has released his rpc lock, will respond {:#?}",
            me, result
        );
        labrpc::Result::Ok(result)
    }

    async fn append_entries(&self, req: AppendEntryRequest) -> labrpc::Result<AppendEntryResponse> {
        info!("inside append_entries rpc with req={:#?}", req);

        let result = AppendEntryResponse {
            term: 0,
            success: true,
        };

        {
            let mut guard = match self.raft.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };

            if !guard.state.is_follower && guard.voted_for.is_some() {
                info!("setting as follower");
                guard.set_state(req.term, false, false, true);
            }
        }

        labrpc::Result::Ok(result)
    }
}
