use flnet::network::{node_connection::NCError};
use flutils::broker::BrokerError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TestError {
    #[error(transparent)]
    Broker(#[from] BrokerError),
    #[error(transparent)]
    NC(#[from] NCError),
}

mod test_webrtc;
mod test_websocket;