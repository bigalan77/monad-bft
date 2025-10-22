use alloy_consensus::TxEnvelope;
use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_eth_txpool_types::DEFAULT_TX_PRIORITY;

#[derive(RlpEncodable, RlpDecodable)]
pub struct EthTxPoolIpcTx {
    pub tx: TxEnvelope,
    pub priority: u64,

    /// Used by forks to pass custom instructions to txpool
    pub extra_data: Vec<u8>,
}

impl EthTxPoolIpcTx {
    pub fn new_with_default_priority(tx: TxEnvelope, extra_data: Vec<u8>) -> Self {
        Self {
            tx,
            priority: DEFAULT_TX_PRIORITY,
            extra_data,
        }
    }
}
