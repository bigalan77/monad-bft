use alloy_consensus::TxEnvelope;
use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_eth_txpool_types::DEFAULT_TX_PRIORITY;

#[derive(RlpEncodable, RlpDecodable)]
pub struct EthTxPoolIpcTx {
    pub tx: TxEnvelope,
    pub priority: u64,
}

impl EthTxPoolIpcTx {
    pub fn new_with_default_priority(tx: TxEnvelope) -> Self {
        Self {
            tx,
            priority: DEFAULT_TX_PRIORITY,
        }
    }
}
