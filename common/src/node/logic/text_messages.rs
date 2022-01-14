use crate::{
    broker::{BInput, BrokerMessage, Subsystem, SubsystemListener},
    node::{network::BrokerNetwork, timer::BrokerTimer, BrokerError, NodeData},
};
use log::{info, trace};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fmt,
    sync::{mpsc::Sender, Arc, Mutex},
};
use thiserror::Error;

use super::messages::{Message, MessageV1};
use crate::{
    node::config::{NodeConfig, NodeInfo},
    types::now,
};
use types::{
    data_storage::{DataStorage, StorageError},
    nodeids::U256,
};

const MESSAGE_MAXIMUM: usize = 20;
const TEXT_MESSAGE_KEY: &str = "text_message";

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TextMessagesStorage {
    pub storage: HashMap<U256, TextMessage>,
}

impl TextMessagesStorage {
    pub fn new() -> Self {
        Self {
            storage: HashMap::new(),
        }
    }

    pub fn load(&mut self, data: &str) -> Result<(), TMError> {
        if data.len() > 0 {
            let msg_vec: Vec<TextMessage> = serde_json::from_str(data)?;
            self.storage.clear();
            for msg in msg_vec {
                self.storage.insert(msg.id(), msg);
            }
        } else {
            self.storage = HashMap::new();
        }
        Ok(())
    }

    pub fn save(&self) -> Result<String, TMError> {
        let msg_vec: Vec<TextMessage> = self.storage.iter().map(|(_k, v)| v).cloned().collect();
        Ok(serde_json::to_string(&msg_vec)?.into())
    }

    // Limit the number of messages to MESSAGE_MAXIMUM,
    // returns the ids of deleted messages.
    pub fn limit_messages(&mut self) -> Vec<U256> {
        if self.storage.len() > MESSAGE_MAXIMUM {
            let mut msgs = self
                .storage
                .iter()
                .map(|(_k, v)| v.clone())
                .collect::<Vec<TextMessage>>();
            msgs.sort_by(|a, b| b.created.partial_cmp(&a.created).unwrap());
            msgs.drain(0..MESSAGE_MAXIMUM);
            for msg in msgs.iter() {
                self.storage.remove(&msg.id());
            }
            msgs.iter().map(|msg| msg.id()).collect()
        } else {
            vec![]
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AddMessage {
    pub msg: String,
}

pub struct TextMessages {
    node_data: Arc<Mutex<NodeData>>,
    storage: Box<dyn DataStorage>,
    broker_tx: Sender<BInput>,
    nodes: Vec<NodeInfo>,
    // maps a message-id to a list of nodes that might hold that message.
    nodes_msgs: HashMap<U256, Vec<U256>>,
    cfg: NodeConfig,
}

#[derive(Error, Debug)]
pub enum TMError {
    #[error("Received an unknown message")]
    UnknownMessage,
    #[error("While sending through output queue")]
    OutputQueue,
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Broker(#[from] BrokerError),
}

impl SubsystemListener for TextMessages {
    fn messages(&mut self, bms: Vec<&BrokerMessage>) -> std::vec::Vec<BInput> {
        for bm in bms {
            match bm {
                BrokerMessage::Timer(BrokerTimer::Second) => {
                    if let Err(e) = self.update_messages() {
                        log::warn!("While updating messages: {:?}", e)
                    }
                }
                BrokerMessage::TextMessage(am) => self.add_message(am.msg.clone()).unwrap(),
                BrokerMessage::Network(BrokerNetwork::UpdateList(nul)) => {
                    if let Err(e) = self.update_nodes(nul) {
                        log::warn!("Couldn't update nodes: {:?}", e);
                    }
                }
                BrokerMessage::NodeMessage(nm) => match &nm.msg {
                    Message::V1(MessageV1::TextMessage(tm)) => {
                        if let Err(e) = self.handle_msg(&nm.id, tm.clone()) {
                            log::warn!("Couldn't handle message: {:?}", e)
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }
        vec![]
    }
}

impl TextMessages {
    pub fn new(node_data: Arc<Mutex<NodeData>>) -> Result<(), TMError> {
        let mut broker = node_data.lock().unwrap().broker.clone();
        Ok(
            broker.add_subsystem(Subsystem::Handler(Box::new(Self::new_self(
                node_data,
                broker.clone_tx(),
            )?)))?,
        )
    }

    fn new_self(
        node_data: Arc<Mutex<NodeData>>,
        broker_tx: Sender<BInput>,
    ) -> Result<Self, TMError> {
        let (cfg, nodes_msgs, storage) = {
            let mut node_data = node_data.lock().unwrap();
            let storage = node_data.storage.get("text_messages");
            let data = storage.get(TEXT_MESSAGE_KEY)?;
            node_data.messages.load(&data)?;
            let mut nodes_msgs = HashMap::new();
            let cfg = node_data.node_config.clone();
            for (_, tm) in node_data.messages.storage.iter() {
                nodes_msgs.insert(tm.id(), vec![cfg.our_node.get_id()]);
            }
            (cfg, nodes_msgs, storage)
        };
        Ok(Self {
            node_data,
            broker_tx,
            nodes_msgs,
            nodes: vec![],
            cfg,
            storage,
        })
    }

    fn handle_msg(&mut self, from: &U256, msg: TextMessageV1) -> Result<(), TMError> {
        match msg {
            TextMessageV1::List() => {
                let ids = self
                    .nodes_msgs
                    .clone()
                    .into_iter()
                    .map(|(id, nodes)| TextStorage { id, nodes })
                    .collect();
                self.send(from, TextMessageV1::IDs(ids))
            }
            TextMessageV1::Get(id) => {
                if let Some(text) = self.node_data.lock().unwrap().messages.storage.get(&id) {
                    // Suppose that the other node will also have it, so add it to the nodes_msgs
                    let nm = self
                        .nodes_msgs
                        .entry(id)
                        .or_insert(vec![self.cfg.our_node.get_id()]);
                    nm.push(from.clone());
                    self.send(from, TextMessageV1::Text(text.clone()))
                } else {
                    Err(TMError::UnknownMessage)
                }
            }
            TextMessageV1::Set(text) => {
                // Create a list of nodes, starting with the sender node.
                let mut list = vec![from.clone()];
                for node in self.nodes.iter().filter(|n| &n.get_id() != from) {
                    // Send the text to all nodes other than the sender node and ourselves.
                    self.send(&node.get_id(), TextMessageV1::Text(text.clone()))?;
                    list.push(node.get_id().clone());
                }
                list.push(self.cfg.our_node.get_id().clone());
                self.nodes_msgs.insert(text.id(), list);
                self.insert_message(text)?;
                Ok(())
            }
            // Currently we just suppose that there is a central node storing all node-ids
            // and texts, so we can simply overwrite the ones we already have.
            TextMessageV1::IDs(list) => {
                // Only ask messages we don't have yet.
                for ts in list.iter().filter(|ts| {
                    !self
                        .node_data
                        .lock()
                        .unwrap()
                        .messages
                        .storage
                        .contains_key(&ts.id)
                }) {
                    info!("Found IDs without messages - asking nodes {:?}", ts.nodes);
                    // TODO: only ask other nodes if the first node didn't answer
                    for node in ts.nodes.iter() {
                        self.send(node, TextMessageV1::Get(ts.id.clone()))?;
                    }
                }
                Ok(())
            }
            // Sets a received text, and also creates an entry in the self.nodes_msgs to indicate that
            // we do know about this message.
            TextMessageV1::Text(tm) => {
                let tmid = tm.id();
                let mut prev_nodes = self.nodes_msgs.remove(&tmid).unwrap_or(vec![]);
                if prev_nodes
                    .iter()
                    .find(|&id| id == &self.cfg.our_node.get_id())
                    .is_none()
                {
                    prev_nodes.push(self.cfg.our_node.get_id());
                }
                self.nodes_msgs.insert(tmid, prev_nodes.clone());
                self.insert_message(tm)?;

                Ok(())
            }
        }
    }

    /// Updates all known nodes. Will send out requests to new nodes to know what
    /// messages are available in those nodes.
    /// Only nodes different from this one will be stored.
    /// Only new leaders will be asked for new messages.
    fn update_nodes(&mut self, nodes: &Vec<NodeInfo>) -> Result<(), TMError> {
        trace!("{} update_nodes", self.cfg.our_node.info);
        let new_nodes: Vec<NodeInfo> = nodes
            .iter()
            .filter(|n| n.get_id() != self.cfg.our_node.get_id())
            .cloned()
            .collect();

        // Contact only new leaders
        for leader in new_nodes
            .iter()
            .filter(|n| !self.nodes.contains(n))
            .filter(|n| n.node_capacities.leader == true)
        {
            self.send(&leader.get_id(), TextMessageV1::List())?;
        }

        // Store new nodes, overwrite previous nodes
        trace!("new nodes are: {:?}", new_nodes);
        self.nodes = new_nodes;
        Ok(())
    }

    /// Asks all nodes for new messages.
    fn update_messages(&mut self) -> Result<(), TMError> {
        for node in self.nodes.iter() {
            self.send(&node.get_id(), TextMessageV1::List())?;
        }
        Ok(())
    }

    /// Adds a new message to the list of messages and sends it to the leaders.
    fn add_message(&mut self, msg: String) -> Result<(), TMError> {
        let tm = TextMessage {
            node_info: self.cfg.our_node.info.clone(),
            src: self.cfg.our_node.get_id(),
            created: now(),
            liked: 0,
            msg,
        };
        self.nodes_msgs
            .insert(tm.id(), vec![self.cfg.our_node.get_id()]);
        self.insert_message(tm.clone())?;

        for leader in self.get_leaders() {
            info!("Sending message to leader {:?}", leader);
            self.send(&leader.get_id(), TextMessageV1::Set(tm.clone()))?;
        }
        Ok(())
    }

    fn get_leaders(&self) -> Vec<NodeInfo> {
        self.nodes
            .iter()
            .filter(|n| n.node_capacities.leader)
            .cloned()
            .collect()
    }

    // Wrapper call to send things over the Broker
    fn send(&self, to: &U256, msg: TextMessageV1) -> Result<(), TMError> {
        let m = Message::V1(MessageV1::TextMessage(msg));
        let str = serde_json::to_string(&m)?;
        self.broker_tx
            .send(BInput::BM(BrokerMessage::Network(BrokerNetwork::WebRTC(
                to.clone(),
                str,
            ))))
            .map_err(|_| TMError::OutputQueue)
    }

    fn insert_message(&mut self, text: TextMessage) -> Result<(), TMError> {
        let mut node_data = self.node_data.lock().expect("Getting node_data");
        node_data.messages.storage.insert(text.id(), text);
        for msg in node_data.messages.limit_messages() {
            self.nodes_msgs.remove(&msg);
        }

        let data = node_data.messages.save()?;
        Ok(self.storage.set(TEXT_MESSAGE_KEY, &data)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TextMessageV1 {
    // Requests the updated list of all TextIDs available. This is best
    // sent to one of the Oracle Servers.
    List(),
    // Request a text from a node. If the node doesn't have this text
    // available, it should respond with an Error.
    Get(U256),
    // Stores a new text on the Oracle Servers
    Set(TextMessage),
    // Tuples of [NodeID ; TextID] indicating texts and where they should
    // be read from.
    // The NodeID can change from one TextIDsGet call to another, as nodes
    // can be coming and going.
    IDs(Vec<TextStorage>),
    // The Text as known by the node.
    Text(TextMessage),
}

impl fmt::Display for TextMessageV1 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TextMessageV1::List() => write!(f, "List"),
            TextMessageV1::Get(_) => write!(f, "Get"),
            TextMessageV1::Set(_) => write!(f, "Set"),
            TextMessageV1::IDs(_) => write!(f, "IDs"),
            TextMessageV1::Text(_) => write!(f, "Text"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TextStorage {
    pub id: U256,
    pub nodes: Vec<U256>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TextMessage {
    pub node_info: String,
    pub src: U256,
    pub created: f64,
    pub liked: u64,
    pub msg: String,
}

impl TextMessage {
    pub fn id(&self) -> U256 {
        let mut id = Sha256::new();
        id.update(&self.src);
        id.update(&self.created.to_le_bytes());
        id.update(&self.liked.to_le_bytes());
        id.update(&self.msg);
        id.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        broker::{BInput, BrokerMessage},
        node::{logic::text_messages::TextMessagesStorage, node_data::TempDSB, NodeData, TMError},
    };
    use anyhow::Result;
    use flexi_logger::LevelFilter;
    use log::{debug, info, warn};
    use std::sync::{
        mpsc::{channel, Receiver, TryRecvError},
        Arc, Mutex,
    };

    use super::{Message, TextMessage, TextMessages};
    use crate::node::{config::NodeConfig, logic::messages::MessageV1};
    use types::nodeids::U256;

    struct TMTest {
        tm: TextMessages,
        rx: Receiver<BInput>,
        cfg: NodeConfig,
        nd: Arc<Mutex<NodeData>>,
    }

    impl TMTest {
        pub fn new(leader: bool) -> Result<Self, TMError> {
            let mut cfg = NodeConfig::new();
            cfg.our_node.node_capacities.leader = leader;
            let nd = NodeData::new(cfg.clone(), TempDSB::new());
            let (tx, rx) = channel::<BInput>();
            Ok(TMTest {
                tm: TextMessages::new_self(Arc::clone(&nd), tx)?,
                rx,
                cfg,
                nd,
            })
        }

        pub fn empty_queue(&mut self) -> bool {
            matches!(self.rx.try_recv(), Err(TryRecvError::Empty))
        }
    }

    /// Processes all messages from all nodes until no messages are left.
    fn process(tms: &mut Vec<&mut TMTest>) -> Result<()> {
        loop {
            let mut msgs: Vec<(U256, U256, Message)> = vec![];
            for tm in tms.iter() {
                for bmin in tm.rx.try_iter() {
                    if let BInput::BM(bm) = bmin {
                        match bm {
                            BrokerMessage::NodeMessage(nm) => {
                                msgs.push((tm.cfg.our_node.get_id(), nm.id, nm.msg))
                            }
                            _ => warn!("Unsupported message received"),
                        }
                    }
                }
            }

            if msgs.len() == 0 {
                debug!("No messages found - stopping");
                break;
            }

            for (from, to, msg) in msgs {
                if let Some(tm) = tms.into_iter().find(|tm| tm.cfg.our_node.get_id() == to) {
                    match msg {
                        Message::V1(msg_send) => match msg_send {
                            MessageV1::TextMessage(smv) => tm.tm.handle_msg(&from, smv)?,
                            _ => warn!("Ignoring message {:?}", msg_send),
                        },
                        Message::Unknown(s) => warn!("Got Message::Unknown({})", s),
                    }
                } else {
                    warn!("Got message for unknown node {:?}", to)
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_new_msg() -> Result<()> {
        simple_logging::log_to_stderr(LevelFilter::Trace);

        let mut leader = TMTest::new(true)?;
        let mut follower1 = TMTest::new(false)?;
        let mut follower2 = TMTest::new(false)?;

        let all_nodes = vec![
            leader.cfg.our_node.clone(),
            follower1.cfg.our_node.clone(),
            follower2.cfg.our_node.clone(),
        ];

        assert_eq!(leader.nd.lock().unwrap().messages.storage.len(), 0);
        assert_eq!(follower1.nd.lock().unwrap().messages.storage.len(), 0);
        assert_eq!(follower2.nd.lock().unwrap().messages.storage.len(), 0);

        info!("Adding a first message to the follower 1");
        let msg1 = String::from("1st Message");
        follower1.tm.add_message(msg1.clone())?;
        if !follower1.empty_queue() {
            panic!("queue should be empty");
        }

        info!("Add a new message to follower 1 and verify it's stored in the leader");
        follower1.tm.update_nodes(&all_nodes)?;
        leader.tm.update_nodes(&all_nodes[0..2].to_vec())?;
        if !leader.empty_queue() {
            panic!("leader queue should be empty");
        }
        let msg2 = String::from("2nd Message");
        follower1.tm.add_message(msg2.clone())?;
        process(&mut vec![&mut follower1, &mut leader])?;
        assert_eq!(1, leader.nd.lock().unwrap().messages.storage.len());
        let tm1_id = leader
            .tm
            .node_data
            .lock()
            .unwrap()
            .messages
            .storage
            .keys()
            .next()
            .unwrap()
            .clone();
        assert_eq!(2, leader.tm.nodes_msgs.get(&tm1_id).unwrap().len());

        info!("Let the follower2 catch up on the new messages");
        follower2.tm.update_nodes(&all_nodes)?;
        process(&mut vec![&mut follower1, &mut follower2, &mut leader])?;
        assert_eq!(
            follower2
                .tm
                .node_data
                .lock()
                .unwrap()
                .messages
                .storage
                .len(),
            1
        );
        assert_eq!(2, follower1.tm.nodes_msgs.get(&tm1_id).unwrap().len());

        // Follower2 also creates their message
        info!("Follower2 creates new message");
        let msg3 = String::from("3rd Message");
        leader.tm.update_nodes(&all_nodes)?;
        follower2.tm.add_message(msg3)?;
        process(&mut vec![&mut follower1, &mut follower2, &mut leader])?;
        for msg in &follower1.nd.lock().unwrap().messages.storage {
            info!("Message is: {:?}", msg.1);
        }
        assert_eq!(
            follower1
                .tm
                .node_data
                .lock()
                .unwrap()
                .messages
                .storage
                .len(),
            3
        );

        Ok(())
    }

    #[test]
    fn test_update_nodes() -> Result<()> {
        simple_logging::log_to_stderr(LevelFilter::Trace);

        let leader = TMTest::new(true)?;
        let mut follower1 = TMTest::new(false)?;

        let all_nodes = vec![leader.cfg.our_node.clone(), follower1.cfg.our_node.clone()];

        // Update nodes twice - first should send a message to the leader,
        // second update_nodes should come out empty, because no new
        // leader is found.
        follower1.tm.update_nodes(&all_nodes)?;
        if follower1.empty_queue() {
            panic!("queue should have one message");
        }
        assert_eq!(1, follower1.tm.nodes.len());

        follower1.tm.update_nodes(&all_nodes)?;
        if !follower1.empty_queue() {
            panic!("queue should be empty now");
        }
        assert_eq!(1, follower1.tm.nodes.len());

        follower1.tm.update_nodes(&vec![])?;
        assert_eq!(0, follower1.tm.nodes.len());

        Ok(())
    }

    #[test]
    fn test_id() {
        let tm1 = TextMessage {
            node_info: String::from(""),
            src: U256::rnd(),
            created: 0f64,
            liked: 0u64,
            msg: "test message".to_string(),
        };
        assert_eq!(tm1.id(), tm1.id());

        let mut tm2 = tm1.clone();
        tm2.src = U256::rnd();
        assert_ne!(tm1.id(), tm2.id());

        tm2 = tm1.clone();
        tm2.created = 1f64;
        assert_ne!(tm1.id(), tm2.id());

        tm2 = tm1.clone();
        tm2.liked = 1u64;
        assert_ne!(tm1.id(), tm2.id());

        tm2 = tm1.clone();
        tm2.msg = "short test".to_string();
        assert_ne!(tm1.id(), tm2.id());
    }

    #[test]
    fn test_load_save() -> Result<(), TMError> {
        simple_logging::log_to_stderr(LevelFilter::Trace);

        let mut tms = TextMessagesStorage::new();
        let tm1 = TextMessage {
            node_info: String::from(""),
            src: U256::rnd(),
            created: 0f64,
            liked: 0u64,
            msg: "test message".to_string(),
        };
        tms.storage.insert(tm1.id(), tm1);

        let data = tms.save()?;

        let mut tms_clone = TextMessagesStorage::new();
        tms_clone.load(&data)?;
        assert_eq!(tms, tms_clone);
        Ok(())
    }
}
