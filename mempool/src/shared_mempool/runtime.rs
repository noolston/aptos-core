// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    core_mempool::CoreMempool,
    network::MempoolSyncMsg,
    shared_mempool::{
        broadcast_peers_selector::{
            AllPeersSelector, BroadcastPeersSelector, FreshPeersSelector, PrioritizedPeersSelector,
        },
        coordinator::{coordinator, gc_coordinator, snapshot_job},
        types::{MempoolEventsReceiver, SharedMempool, SharedMempoolNotification},
    },
    QuorumStoreRequest,
};
use aptos_config::config::NodeConfig;
use aptos_data_client::interface::AptosPeersInterface;
use aptos_event_notifications::ReconfigNotificationListener;
use aptos_infallible::{Mutex, RwLock};
use aptos_logger::Level;
use aptos_mempool_notifications::MempoolNotificationListener;
use aptos_network::application::interface::{NetworkClient, NetworkServiceEvents};
use aptos_storage_interface::DbReader;
use aptos_vm_validator::vm_validator::{TransactionValidation, VMValidator};
use futures::channel::mpsc::{Receiver, UnboundedSender};
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};

/// Bootstrap of SharedMempool.
/// Creates a separate Tokio Runtime that runs the following routines:
///   - outbound_sync_task (task that periodically broadcasts transactions to peers).
///   - inbound_network_task (task that handles inbound mempool messages and network events).
///   - gc_task (task that performs GC of all expired transactions by SystemTTL).
#[allow(clippy::too_many_arguments)]
pub(crate) fn start_shared_mempool<TransactionValidator>(
    executor: &Handle,
    config: &NodeConfig,
    mempool: Arc<Mutex<CoreMempool>>,
    network_client: NetworkClient<MempoolSyncMsg>,
    network_service_events: NetworkServiceEvents<MempoolSyncMsg>,
    client_events: MempoolEventsReceiver,
    quorum_store_requests: Receiver<QuorumStoreRequest>,
    mempool_listener: MempoolNotificationListener,
    mempool_reconfig_events: ReconfigNotificationListener,
    db: Arc<dyn DbReader>,
    validator: Arc<RwLock<TransactionValidator>>,
    subscribers: Vec<UnboundedSender<SharedMempoolNotification>>,
    peers: Arc<dyn AptosPeersInterface>,
    broadcast_peers_selector: Arc<RwLock<Box<dyn BroadcastPeersSelector>>>,
) where
    TransactionValidator: TransactionValidation + 'static,
{
    let smp: SharedMempool<NetworkClient<MempoolSyncMsg>, TransactionValidator> =
        SharedMempool::new(
            mempool.clone(),
            config.mempool.clone(),
            network_client,
            db,
            validator,
            subscribers,
            config.base.role,
        );

    executor.spawn(coordinator(
        smp,
        executor.clone(),
        network_service_events,
        client_events,
        quorum_store_requests,
        mempool_listener,
        mempool_reconfig_events,
        config.mempool.peer_update_interval_ms,
        peers,
        broadcast_peers_selector,
    ));

    executor.spawn(gc_coordinator(
        mempool.clone(),
        config.mempool.system_transaction_gc_interval_ms,
    ));

    if aptos_logger::enabled!(Level::Trace) {
        executor.spawn(snapshot_job(
            mempool,
            config.mempool.mempool_snapshot_interval_secs,
        ));
    }
}

pub fn bootstrap(
    config: &NodeConfig,
    db: Arc<dyn DbReader>,
    network_client: NetworkClient<MempoolSyncMsg>,
    network_service_events: NetworkServiceEvents<MempoolSyncMsg>,
    client_events: MempoolEventsReceiver,
    quorum_store_requests: Receiver<QuorumStoreRequest>,
    mempool_listener: MempoolNotificationListener,
    mempool_reconfig_events: ReconfigNotificationListener,
    peers: Arc<dyn AptosPeersInterface>,
) -> Runtime {
    let runtime = aptos_runtimes::spawn_named_runtime("shared-mem".into(), None);

    let broadcast_peers_selector = {
        let inner_selector: Box<dyn BroadcastPeersSelector> = if config.base.role.is_validator() {
            Box::new(AllPeersSelector::new())
        } else if peers.is_vfn() {
            Box::new(PrioritizedPeersSelector::new(config.mempool.clone()))
        } else {
            Box::new(FreshPeersSelector::new(config.mempool.clone()))
        };
        Arc::new(RwLock::new(inner_selector))
    };

    let mempool = Arc::new(Mutex::new(CoreMempool::new(
        config,
        broadcast_peers_selector.clone(),
    )));
    let vm_validator = Arc::new(RwLock::new(VMValidator::new(Arc::clone(&db))));
    start_shared_mempool(
        runtime.handle(),
        config,
        mempool,
        network_client,
        network_service_events,
        client_events,
        quorum_store_requests,
        mempool_listener,
        mempool_reconfig_events,
        db,
        vm_validator,
        vec![],
        peers,
        broadcast_peers_selector,
    );
    runtime
}
