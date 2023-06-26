// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use aptos_indexer_grpc_post_processor::{
    file_storage_verifier::FileStorageVerifier, pfn_ledger_checker::PfnLedgerChecker,
};
use aptos_indexer_grpc_server_framework::{RunnableConfig, ServerArgs};
use aptos_indexer_grpc_utils::config::IndexerGrpcFileStoreConfig;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerGrpcPFNCheckerConfig {
    pub public_fullnode_address: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerGrpcFileStorageVerifierConfig {
    pub file_store_config: IndexerGrpcFileStoreConfig,
    pub chain_id: u64,
}

// TODO: change this to match pattern.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerGrpcPostProcessorConfig {
    pub pfn_checker_config: Option<IndexerGrpcPFNCheckerConfig>,
    pub file_storage_verifier: Option<IndexerGrpcFileStorageVerifierConfig>,
}

#[async_trait::async_trait]
impl RunnableConfig for IndexerGrpcPostProcessorConfig {
    async fn run(&self) -> Result<()> {
        let mut tasks = vec![];
        if let Some(config) = &self.pfn_checker_config {
            tasks.push(tokio::spawn({
                let config = config.clone();
                async move {
                    let checker = PfnLedgerChecker::new(config.public_fullnode_address.clone());
                    info!("Starting PfnLedgerChecker");
                    checker.run().await
                }
            }));
        }

        if let Some(config) = &self.file_storage_verifier {
            tasks.push(tokio::spawn({
                let config = config.clone();
                async move {
                    let checker =
                        FileStorageVerifier::new(config.file_store_config.clone(), config.chain_id);
                    info!("Starting FileStorageVerifier");
                    checker.run().await.map_err(|e| {
                        error!("FileStorageVerifier finished with error: {:?}", e);
                        // explicit panic to make sure the process exits with non-zero code.
                        panic!();
                    })
                }
            }));
        }

        let res = futures::future::join_all(tasks).await;

        for r in res {
            if let Err(e) = r {
                error!("Task finished with error: {:?}", e);
                panic!();
            }
        }
        error!("All tasks finished unexpectedly");
        panic!();
    }

    fn get_server_name(&self) -> String {
        "idxbg".to_string()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = ServerArgs::parse();
    args.run::<IndexerGrpcPostProcessorConfig>().await
}
