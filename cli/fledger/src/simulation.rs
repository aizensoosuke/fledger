use crate::{metrics::Metrics, Fledger};
use clap::{arg, Args, Subcommand};
use flarch::{nodeids::U256, tasks::wait_ms};
use flmodules::gossip_events::core::Event;
use metrics::{absolute_counter, increment_counter};

#[derive(Args, Debug, Clone)]
pub struct SimulationCommand {
    /// Print new messages as they come
    #[arg(long, default_value = "false")]
    print_new_messages: bool,

    #[command(subcommand)]
    pub subcommand: SimulationSubcommand,
}

#[derive(Subcommand, Debug, Clone)]
pub enum SimulationSubcommand {
    Chat {
        /// Send a chat message upon node creation
        #[arg(long)]
        send_msg: Option<String>,

        /// Wait for a chat message with the given body.
        /// log "RECV_CHAT_MSG TRIGGERED" upon message received, at log level info
        #[arg(long)]
        recv_msg: Option<String>,
    },

    DhtJoinRealm {},
}

pub struct SimulationHandler {}

impl SimulationHandler {
    pub async fn run(f: Fledger, command: SimulationCommand) -> anyhow::Result<()> {
        match command.subcommand.clone() {
            SimulationSubcommand::Chat { send_msg, recv_msg } => {
                Self::run_chat(f, command, send_msg, recv_msg).await
            }
            SimulationSubcommand::DhtJoinRealm {} => Self::run_dht_join_realm(f).await,
        }
    }

    async fn run_chat(
        mut f: Fledger,
        simulation_args: SimulationCommand,
        send_msg: Option<String>,
        recv_msg: Option<String>,
    ) -> anyhow::Result<()> {
        f.loop_node(crate::FledgerState::Connected(1)).await?;

        let node_name = f.args.name.clone().unwrap_or("unknown".into());

        if let Some(ref msg) = recv_msg {
            log::info!("Waiting for chat message {}.", msg);
        }

        if let Some(ref msg) = send_msg {
            log::info!("Sending chat message {}.", msg);
            f.node.add_chat_message(msg.into()).await?;
        }

        let mut acked_msg_ids: Vec<U256> = Vec::new();

        // necessary to grab the variable for lifetime purposes.
        let _influx = Metrics::setup(node_name);

        loop {
            wait_ms(1000).await;

            let fledger_message_total = f.node.gossip.as_ref().unwrap().chat_events().len();
            let fledger_connected_total = f.node.nodes_connected()?.len();
            absolute_counter!(
                "fledger_message_total",
                fledger_message_total.try_into().unwrap()
            );
            absolute_counter!("fledger_connected_total", fledger_connected_total as u64);
            increment_counter!("fledger_iterations_total");

            if simulation_args.print_new_messages {
                Self::log_new_messages(&f, &mut acked_msg_ids);
            }

            if let Some(ref msg) = recv_msg {
                let gossip = f.node.gossip.as_ref();
                if gossip
                    .unwrap()
                    .chat_events()
                    .iter()
                    .any(|ev| ev.msg.eq(msg))
                {
                    log::info!("RECV_CHAT_MSG TRIGGERED");
                    f.loop_node(crate::FledgerState::Forever).await?;
                    return Ok(());
                }
            }
        }
    }

    async fn run_dht_join_realm(mut f: Fledger) -> anyhow::Result<()> {
        f.loop_node(crate::FledgerState::DHTAvailable).await?;
        log::info!("SIMULATION END");

        f.loop_node(crate::FledgerState::Forever).await?;
        return Ok(());

        // let router = f.node.dht_router.unwrap();
        // let ds = f.node.dht_storage.unwrap();
        // let rv = RealmView::new_first(ds.clone()).await?;

        // To send requests for random floID
        // let realm_id = ds.get_realm_ids().await?.first().unwrap();
        // let res = ds.get_flo(&GlobalID::new(realm_id, FloID::rnd())).await;
        //
        // Alternative to get realm ID
        // rv.realm.realm_id();

        // Send a Flo tag blob
        // if let Some(ref content) = put_content {
        //     log::info!("Storing content in DHT {}.", content);
        //     rv.create_tag(content, None, flcrypto::access::Condition::Pass, &[]);
        //     ds.propagate().await?;
        // }

        // rv.update_tags();
        // rv.tags
        //     .unwrap()
        //     .storage
        //     .iter()
        //     .any(|tag| tag.1.cache().0.values().get(name).is_some());
        // {
        //     log::info!("SIMULATION TRIGGER");
        //     f.loop_node(crate::FledgerState::Forever).await?;
        //     return Ok(());
        // }
    }

    fn log_new_messages(f: &Fledger, acked_msg_ids: &mut Vec<U256>) {
        let chat_events = f.node.gossip.as_ref().unwrap().chat_events();
        let chats: Vec<&Event> = chat_events
            .iter()
            .filter(|ev| !acked_msg_ids.contains(&ev.get_id()))
            .collect();

        if chats.len() <= 0 {
            log::debug!("... No new message");
        } else {
            log::info!("--- New Messages ---");
            for chat in chats {
                acked_msg_ids.push(chat.get_id());
                log::info!("    [{}] {}", chat.src, chat.msg);
            }
        }
    }
}
