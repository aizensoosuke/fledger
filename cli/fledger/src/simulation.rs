use crate::{metrics::Metrics, Fledger};
use clap::{arg, Subcommand};
use flarch::{nodeids::U256, tasks::wait_ms};
use flmodules::gossip_events::core::Event;
use metrics::{absolute_counter, increment_counter};

#[derive(Subcommand, Debug, Clone)]
pub enum SimulationCommands {
    /// Send a chat message upon node creation
    SendChat { msg: String },
    /// Wait for a chat message with the given body.
    /// log "RECV_CHAT_MSG TRIGGERED" upon message received, at log level info
    RecvChat {
        msg: String,

        /// Print new messages as they come
        #[arg(long, default_value = "false")]
        print_new_messages: bool,

        /// Node's name
        #[arg(long, default_value = "unknown")]
        node_name: String,
    },
}

pub struct SimulationHandler {}

impl SimulationHandler {
    pub async fn run(f: Fledger, command: SimulationCommands) -> anyhow::Result<()> {
        match command {
            SimulationCommands::SendChat { msg } => Self::simulation_send_chat(f, msg).await,
            SimulationCommands::RecvChat {
                msg,
                print_new_messages,
                node_name,
            } => Self::simulation_recv_chat(f, msg, print_new_messages, node_name).await,
        }
    }

    async fn simulation_send_chat(mut f: Fledger, msg: String) -> anyhow::Result<()> {
        f.loop_node(crate::FledgerState::Connected(1)).await?;

        log::info!("Sending chat message {}.", msg);
        f.node.add_chat_message(msg).await?;

        f.loop_node(crate::FledgerState::Forever).await?;
        Ok(())
    }

    async fn simulation_recv_chat(
        mut f: Fledger,
        msg: String,
        print_new_messages: bool,
        node_name: String,
    ) -> anyhow::Result<()> {
        f.loop_node(crate::FledgerState::Connected(1)).await?;

        log::info!("Waiting for chat message {}.", msg);

        let mut acked_msg_ids: Vec<U256> = Vec::new();

        // necessary to grab the variable for lifetime purposes.
        let _influx = Metrics::setup(node_name);

        loop {
            wait_ms(1000).await;

            let fledger_message_total = f.node.gossip.as_ref().unwrap().chat_events().len();
            absolute_counter!(
                "fledger_message_total",
                fledger_message_total.try_into().unwrap()
            );
            increment_counter!("fledger_iterations_total");

            if print_new_messages {
                Self::log_new_messages(&f, &mut acked_msg_ids);
            }

            let gossip = f.node.gossip.as_ref();
            if gossip.unwrap().chat_events().iter().any(|ev| ev.msg == msg) {
                log::info!("RECV_CHAT_MSG TRIGGERED");
                f.loop_node(crate::FledgerState::Forever).await?;
                return Ok(());
            }
        }
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
