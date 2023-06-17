// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    error::Error,
    moderator::RequestModerator,
    network::ResponseSender,
    storage::StorageReader,
    subscription,
    subscription::{SubscriptionRequest, SubscriptionStreamRequests, MAX_NUM_ACTIVE_SUBSCRIPTIONS},
    tests::{mock, mock::MockClient, utils},
};
use aptos_config::{config::StorageServiceConfig, network_id::PeerNetworkId};
use aptos_infallible::{Mutex, RwLock};
use aptos_logger::info;
use aptos_storage_service_types::{
    requests::{
        DataRequest, StorageServiceRequest, SubscribeTransactionOutputsWithProofRequest,
        SubscribeTransactionsOrOutputsWithProofRequest, SubscribeTransactionsWithProofRequest,
    },
    responses::{CompleteDataRange, StorageServerSummary},
    StorageServiceError,
};
use aptos_time_service::TimeService;
use aptos_types::{epoch_change::EpochChangeProof, ledger_info::LedgerInfoWithSignatures};
use futures::channel::oneshot;
use lru::LruCache;
use std::{collections::HashMap, sync::Arc};

#[tokio::test]
async fn test_peers_with_ready_subscriptions() {
    // Create a mock time service and subscriptions map
    let time_service = TimeService::mock();
    let subscriptions = Arc::new(Mutex::new(HashMap::new()));

    // Create three peers with ready subscriptions
    let mut peer_network_ids = vec![];
    for known_version in &[1, 5, 10] {
        // Create a random peer network id
        let peer_network_id = PeerNetworkId::random();
        peer_network_ids.push(peer_network_id);

        // Create a subscription stream and insert it into the pending map
        let subscription_stream_requests = create_subscription_stream_requests(
            time_service.clone(),
            Some(*known_version),
            Some(1),
            Some(0),
            Some(0),
        );
        subscriptions
            .lock()
            .insert(peer_network_id, subscription_stream_requests);
    }

    // Create epoch ending test data at version 9
    let epoch_ending_ledger_info = utils::create_epoch_ending_ledger_info(1, 9);
    let epoch_change_proof = EpochChangeProof {
        ledger_info_with_sigs: vec![epoch_ending_ledger_info],
        more: false,
    };

    // Create the mock db reader
    let mut db_reader = mock::create_mock_db_reader();
    utils::expect_get_epoch_ending_ledger_infos(&mut db_reader, 1, 2, epoch_change_proof);

    // Create the storage reader
    let storage_reader = StorageReader::new(StorageServiceConfig::default(), Arc::new(db_reader));

    // Create test data with an empty storage server summary
    let cached_storage_server_summary = Arc::new(RwLock::new(StorageServerSummary::default()));
    let optimistic_fetches = Arc::new(Mutex::new(HashMap::new()));
    let lru_response_cache = Arc::new(Mutex::new(LruCache::new(0)));
    let request_moderator = Arc::new(RequestModerator::new(
        cached_storage_server_summary.clone(),
        mock::create_peers_and_metadata(vec![]),
        StorageServiceConfig::default(),
        time_service.clone(),
    ));

    // Verify that there are no peers with ready subscriptions
    let peers_with_ready_subscriptions = subscription::get_peers_with_ready_subscriptions(
        cached_storage_server_summary.clone(),
        optimistic_fetches.clone(),
        subscriptions.clone(),
        lru_response_cache.clone(),
        request_moderator.clone(),
        storage_reader.clone(),
        time_service.clone(),
    )
    .unwrap();
    assert!(peers_with_ready_subscriptions.is_empty());

    // Update the storage server summary so that there is new data (at version 2)
    let highest_synced_ledger_info =
        update_storage_summary(2, 1, cached_storage_server_summary.clone());

    // Verify that peer 1 has a ready subscription
    let peers_with_ready_subscriptions = subscription::get_peers_with_ready_subscriptions(
        cached_storage_server_summary.clone(),
        optimistic_fetches.clone(),
        subscriptions.clone(),
        lru_response_cache.clone(),
        request_moderator.clone(),
        storage_reader.clone(),
        time_service.clone(),
    )
    .unwrap();
    assert_eq!(peers_with_ready_subscriptions, vec![(
        peer_network_ids[0],
        highest_synced_ledger_info
    )]);

    // Manually remove subscription 1 from the map
    subscriptions.lock().remove(&peer_network_ids[0]);

    // Update the storage server summary so that there is new data (at version 8)
    let highest_synced_ledger_info =
        update_storage_summary(8, 1, cached_storage_server_summary.clone());

    // Verify that peer 2 has a ready subscription
    let peers_with_ready_subscriptions = subscription::get_peers_with_ready_subscriptions(
        cached_storage_server_summary.clone(),
        optimistic_fetches.clone(),
        subscriptions.clone(),
        lru_response_cache.clone(),
        request_moderator.clone(),
        storage_reader.clone(),
        time_service.clone(),
    )
    .unwrap();
    assert_eq!(peers_with_ready_subscriptions, vec![(
        peer_network_ids[1],
        highest_synced_ledger_info
    )]);

    // Manually remove subscription 2 from the map
    subscriptions.lock().remove(&peer_network_ids[1]);

    // Update the storage server summary so that there is new data (at version 100)
    let _ = update_storage_summary(100, 2, cached_storage_server_summary.clone());

    // Verify that subscription 3 is not returned because it was invalid
    let peers_with_ready_subscriptions = subscription::get_peers_with_ready_subscriptions(
        cached_storage_server_summary,
        optimistic_fetches,
        subscriptions.clone(),
        lru_response_cache,
        request_moderator,
        storage_reader,
        time_service,
    )
    .unwrap();
    assert_eq!(peers_with_ready_subscriptions, vec![]);

    // Verify that the subscriptions are now empty
    assert!(subscriptions.lock().is_empty());
}

#[tokio::test]
async fn test_remove_expired_subscriptions_no_new_data() {
    // Create a storage service config
    let max_subscription_period_ms = 100;
    let storage_service_config = StorageServiceConfig {
        max_subscription_period_ms,
        ..Default::default()
    };

    // Create a mock time service
    let time_service = TimeService::mock();

    // Create the first batch of test subscriptions
    let num_subscriptions_in_batch = 10;
    let subscriptions = Arc::new(Mutex::new(HashMap::new()));
    for _ in 0..num_subscriptions_in_batch {
        let subscription_stream_requests =
            create_subscription_stream_requests(time_service.clone(), None, None, None, None);
        subscriptions
            .lock()
            .insert(PeerNetworkId::random(), subscription_stream_requests);
    }

    // Verify the number of active subscriptions
    assert_eq!(subscriptions.lock().len(), num_subscriptions_in_batch);

    // Elapse a small amount of time (not enough to expire the subscriptions)
    utils::elapse_time(max_subscription_period_ms / 2, &time_service).await;

    // Remove the expired subscriptions and verify none were removed
    subscription::remove_expired_subscription_streams(
        storage_service_config,
        subscriptions.clone(),
    );
    assert_eq!(subscriptions.lock().len(), num_subscriptions_in_batch);

    // Create another batch of test subscriptions
    for _ in 0..num_subscriptions_in_batch {
        let subscription_stream_requests =
            create_subscription_stream_requests(time_service.clone(), None, None, None, None);
        subscriptions
            .lock()
            .insert(PeerNetworkId::random(), subscription_stream_requests);
    }

    // Verify the new number of active subscriptions
    assert_eq!(subscriptions.lock().len(), num_subscriptions_in_batch * 2);

    // Elapse enough time to expire the first batch of subscriptions
    utils::elapse_time(max_subscription_period_ms, &time_service).await;

    // Remove the expired subscriptions and verify the first batch was removed
    subscription::remove_expired_subscription_streams(
        storage_service_config,
        subscriptions.clone(),
    );
    assert_eq!(subscriptions.lock().len(), num_subscriptions_in_batch);

    // Elapse enough time to expire the second batch of subscriptions
    utils::elapse_time(max_subscription_period_ms, &time_service).await;

    // Remove the expired subscriptions and verify the second batch was removed
    subscription::remove_expired_subscription_streams(
        storage_service_config,
        subscriptions.clone(),
    );
    assert!(subscriptions.lock().is_empty());
}

#[tokio::test]
async fn test_remove_expired_subscriptions_blocked_stream() {
    // Create a storage service config
    let max_subscription_period_ms = 100;
    let storage_service_config = StorageServiceConfig {
        max_subscription_period_ms,
        ..Default::default()
    };

    // Create a mock time service
    let time_service = TimeService::mock();

    // Create a batch of test subscriptions
    let num_subscriptions_in_batch = 10;
    let subscriptions = Arc::new(Mutex::new(HashMap::new()));
    let mut peer_network_ids = vec![];
    for i in 0..num_subscriptions_in_batch {
        // Create a new peer
        let peer_network_id = PeerNetworkId::random();
        peer_network_ids.push(peer_network_id);

        // Create a subscription stream request for the peer
        let subscription_stream_requests = create_subscription_stream_requests(
            time_service.clone(),
            Some(1),
            Some(1),
            Some(i as u64),
            Some(0),
        );
        subscriptions
            .lock()
            .insert(peer_network_id, subscription_stream_requests);
    }

    // Create test data with an empty storage server summary
    let cached_storage_server_summary = Arc::new(RwLock::new(StorageServerSummary::default()));
    let optimistic_fetches = Arc::new(Mutex::new(HashMap::new()));
    let lru_response_cache = Arc::new(Mutex::new(LruCache::new(0)));
    let request_moderator = Arc::new(RequestModerator::new(
        cached_storage_server_summary.clone(),
        mock::create_peers_and_metadata(vec![]),
        storage_service_config,
        time_service.clone(),
    ));
    let storage_reader = StorageReader::new(
        storage_service_config,
        Arc::new(mock::create_mock_db_reader()),
    );

    // Update the storage server summary so that there is new data (at version 5)
    let _ = update_storage_summary(5, 1, cached_storage_server_summary.clone());

    // Handle the active subscriptions
    subscription::handle_active_subscriptions(
        cached_storage_server_summary.clone(),
        storage_service_config,
        lru_response_cache.clone(),
        optimistic_fetches.clone(),
        request_moderator.clone(),
        storage_reader.clone(),
        subscriptions.clone(),
        time_service.clone(),
    )
    .unwrap();

    // Verify that all subscription streams are now empty because
    // the pending requests were sent.
    assert_eq!(subscriptions.lock().len(), num_subscriptions_in_batch);
    for (_, subscription_stream_requests) in subscriptions.lock().iter() {
        assert!(subscription_stream_requests
            .first_pending_request()
            .is_none());
    }

    // Elapse enough time to expire the blocked streams
    utils::elapse_time(max_subscription_period_ms + 1, &time_service).await;

    // Add a new subscription request to the first subscription stream
    let subscription_request =
        create_subscription_request(&time_service, Some(1), Some(1), Some(0), Some(1));
    add_subscription_request_to_stream(
        subscription_request,
        subscriptions.clone(),
        &peer_network_ids[0],
    )
    .unwrap();

    // Remove the expired subscriptions and verify all but one were removed
    subscription::remove_expired_subscription_streams(
        storage_service_config,
        subscriptions.clone(),
    );
    assert_eq!(subscriptions.lock().len(), 1);
    assert!(subscriptions.lock().contains_key(&peer_network_ids[0]));
}

#[tokio::test]
async fn test_remove_expired_subscriptions_blocked_stream_index() {
    // Create a storage service config
    let max_subscription_period_ms = 100;
    let storage_service_config = StorageServiceConfig {
        max_subscription_period_ms,
        ..Default::default()
    };

    // Create a mock time service
    let time_service = TimeService::mock();

    // Create the first batch of test subscriptions
    let num_subscriptions_in_batch = 10;
    let subscriptions = Arc::new(Mutex::new(HashMap::new()));
    for _ in 0..num_subscriptions_in_batch {
        let subscription_stream_requests = create_subscription_stream_requests(
            time_service.clone(),
            Some(1),
            Some(1),
            None,
            Some(0),
        );
        subscriptions
            .lock()
            .insert(PeerNetworkId::random(), subscription_stream_requests);
    }

    // Create test data with an empty storage server summary
    let cached_storage_server_summary = Arc::new(RwLock::new(StorageServerSummary::default()));
    let optimistic_fetches = Arc::new(Mutex::new(HashMap::new()));
    let lru_response_cache = Arc::new(Mutex::new(LruCache::new(0)));
    let request_moderator = Arc::new(RequestModerator::new(
        cached_storage_server_summary.clone(),
        mock::create_peers_and_metadata(vec![]),
        storage_service_config,
        time_service.clone(),
    ));
    let storage_reader = StorageReader::new(
        storage_service_config,
        Arc::new(mock::create_mock_db_reader()),
    );

    // Update the storage server summary so that there is new data (at version 5)
    let highest_synced_ledger_info =
        update_storage_summary(5, 1, cached_storage_server_summary.clone());

    // Verify that all peers have ready subscriptions (but don't serve them!)
    let peers_with_ready_subscriptions = subscription::get_peers_with_ready_subscriptions(
        cached_storage_server_summary.clone(),
        optimistic_fetches.clone(),
        subscriptions.clone(),
        lru_response_cache.clone(),
        request_moderator.clone(),
        storage_reader.clone(),
        time_service.clone(),
    )
    .unwrap();
    assert_eq!(
        peers_with_ready_subscriptions.len(),
        num_subscriptions_in_batch
    );

    // Elapse enough time to expire the subscriptions
    utils::elapse_time(max_subscription_period_ms + 1, &time_service).await;

    // Remove the expired subscriptions and verify they were all removed
    subscription::remove_expired_subscription_streams(
        storage_service_config,
        subscriptions.clone(),
    );
    assert!(subscriptions.lock().is_empty());

    // Create another batch of test subscriptions where the stream is
    // blocked on the next index to serve.
    let mut peer_network_ids = vec![];
    for i in 0..num_subscriptions_in_batch {
        // Create a new peer
        let peer_network_id = PeerNetworkId::random();
        peer_network_ids.push(peer_network_id);

        // Create a subscription stream request for the peer
        let subscription_stream_requests = create_subscription_stream_requests(
            time_service.clone(),
            Some(1),
            Some(1),
            None,
            Some(i as u64 + 1),
        );
        subscriptions
            .lock()
            .insert(peer_network_id, subscription_stream_requests);
    }

    // Verify the number of active subscriptions
    assert_eq!(subscriptions.lock().len(), num_subscriptions_in_batch);

    // Verify that none of the subscriptions are ready to be served (they are blocked)
    let peers_with_ready_subscriptions = subscription::get_peers_with_ready_subscriptions(
        cached_storage_server_summary.clone(),
        optimistic_fetches.clone(),
        subscriptions.clone(),
        lru_response_cache.clone(),
        request_moderator.clone(),
        storage_reader.clone(),
        time_service.clone(),
    )
    .unwrap();
    assert!(peers_with_ready_subscriptions.is_empty());

    // Elapse enough time to expire the batch of subscriptions
    utils::elapse_time(max_subscription_period_ms + 1, &time_service).await;

    // Add a new subscription request to the first subscription stream (to unblock it)
    let subscription_request =
        create_subscription_request(&time_service, Some(1), Some(1), None, Some(0));
    add_subscription_request_to_stream(
        subscription_request,
        subscriptions.clone(),
        &peer_network_ids[0],
    )
    .unwrap();

    // Verify that the first peer subscription stream is unblocked
    let peers_with_ready_subscriptions = subscription::get_peers_with_ready_subscriptions(
        cached_storage_server_summary.clone(),
        optimistic_fetches.clone(),
        subscriptions.clone(),
        lru_response_cache.clone(),
        request_moderator.clone(),
        storage_reader.clone(),
        time_service.clone(),
    )
    .unwrap();
    assert_eq!(peers_with_ready_subscriptions.len(), 1);
    assert!(
        peers_with_ready_subscriptions.contains(&(peer_network_ids[0], highest_synced_ledger_info))
    );

    // Remove the expired subscriptions and verify all but one were removed
    subscription::remove_expired_subscription_streams(
        storage_service_config,
        subscriptions.clone(),
    );
    assert_eq!(subscriptions.lock().len(), 1);
    assert!(subscriptions.lock().contains_key(&peer_network_ids[0]));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_max_pending_requests() {
    // Create a storage service config
    let max_transaction_output_chunk_size = 5;
    let storage_service_config = StorageServiceConfig {
        max_transaction_output_chunk_size,
        ..Default::default()
    };

    // Create test data
    let max_num_active_subscriptions = MAX_NUM_ACTIVE_SUBSCRIPTIONS as u64;
    let num_stream_requests = max_num_active_subscriptions * 10; // Send more requests than allowed
    let highest_version = 45576;
    let highest_epoch = 43;
    let lowest_version = 0;
    let peer_version = 50;
    let highest_ledger_info =
        utils::create_test_ledger_info_with_sigs(highest_epoch, highest_version);

    // Create the output lists with proofs
    let output_lists_with_proofs: Vec<_> = (0..num_stream_requests)
        .map(|i| {
            let start_version = peer_version + (i * max_transaction_output_chunk_size) + 1;
            let end_version = start_version + max_transaction_output_chunk_size - 1;
            utils::create_output_list_with_proof(start_version, end_version, highest_version)
        })
        .collect();

    // Create the mock db reader
    let mut db_reader =
        mock::create_mock_db_for_subscription(highest_ledger_info.clone(), lowest_version);
    for stream_request_index in 0..num_stream_requests {
        utils::expect_get_transaction_outputs(
            &mut db_reader,
            peer_version + (stream_request_index * max_transaction_output_chunk_size) + 1,
            max_transaction_output_chunk_size,
            highest_version,
            output_lists_with_proofs[stream_request_index as usize].clone(),
        );
    }

    // Create the storage client and server
    let (mut mock_client, service, mock_time, _) =
        MockClient::new(Some(db_reader), Some(storage_service_config));
    let active_subscriptions = service.get_subscriptions();
    tokio::spawn(service.start());

    // Send the maximum number of stream requests
    let peer_network_id = PeerNetworkId::random();
    let stream_id = 101;
    let mut response_receivers = HashMap::new();
    for stream_request_index in 0..max_num_active_subscriptions {
        // Send the transaction output subscription request
        let response_receiver = utils::subscribe_to_transaction_outputs_for_peer(
            &mut mock_client,
            peer_version,
            highest_epoch,
            stream_id,
            stream_request_index,
            Some(peer_network_id),
        )
        .await;

        // Save the response receiver
        response_receivers.insert(stream_request_index, response_receiver);
    }

    // Wait until the maximum number of stream requests are active
    utils::wait_for_active_stream_requests(
        active_subscriptions.clone(),
        peer_network_id,
        max_num_active_subscriptions as usize,
    )
    .await;

    // Send another batch of stream requests (to exceed the maximum number of
    // subscriptions), and verify that the client receives a failure for each request.
    for stream_request_index in max_num_active_subscriptions..max_num_active_subscriptions * 2 {
        // Send the transaction output subscription request
        let response_receiver = utils::subscribe_to_transaction_outputs_for_peer(
            &mut mock_client,
            peer_version,
            highest_epoch,
            stream_id,
            stream_request_index,
            Some(peer_network_id),
        )
        .await;

        // Verify that the client receives an invalid request error
        let response = mock_client
            .wait_for_response(response_receiver)
            .await
            .unwrap_err();
        assert!(matches!(response, StorageServiceError::InvalidRequest(_)));
    }

    // Verify the request indices that are pending
    verify_pending_subscription_request_indices(
        active_subscriptions.clone(),
        peer_network_id,
        0,
        max_num_active_subscriptions,
        num_stream_requests,
    );

    // Continuously run the subscription service until all of the responses are sent
    for stream_request_index in 0..max_num_active_subscriptions {
        // Elapse enough time to force the subscription thread to work
        utils::wait_for_subscription_service_to_refresh(&mut mock_client, &mock_time).await;

        // Verify a response is received and that it contains the correct data
        let response_receiver = response_receivers.remove(&stream_request_index).unwrap();
        utils::verify_new_transaction_outputs_with_proof(
            &mut mock_client,
            response_receiver,
            output_lists_with_proofs[stream_request_index as usize].clone(),
            highest_ledger_info.clone(),
        )
        .await;
    }

    // Send another batch of requests for transaction outputs
    let mut response_receivers = HashMap::new();
    for stream_request_index in max_num_active_subscriptions..max_num_active_subscriptions * 2 {
        // Send the transaction output subscription request
        let response_receiver = utils::subscribe_to_transaction_outputs_for_peer(
            &mut mock_client,
            peer_version,
            highest_epoch,
            stream_id,
            stream_request_index,
            Some(peer_network_id),
        )
        .await;

        // Save the response receiver
        response_receivers.insert(stream_request_index, response_receiver);
    }

    // Wait until the maximum number of stream requests are active
    utils::wait_for_active_stream_requests(
        active_subscriptions.clone(),
        peer_network_id,
        max_num_active_subscriptions as usize,
    )
    .await;

    // Send another batch of stream requests (to exceed the maximum number of
    // subscriptions), and verify that the client receives a failure for each request.
    for stream_request_index in max_num_active_subscriptions * 2..max_num_active_subscriptions * 3 {
        // Send the transaction output subscription request
        let response_receiver = utils::subscribe_to_transaction_outputs_for_peer(
            &mut mock_client,
            peer_version,
            highest_epoch,
            stream_id,
            stream_request_index,
            Some(peer_network_id),
        )
        .await;

        // Verify that the client receives an invalid request error
        let response = mock_client
            .wait_for_response(response_receiver)
            .await
            .unwrap_err();
        assert!(matches!(response, StorageServiceError::InvalidRequest(_)));
    }

    // Verify the request indices that are pending
    verify_pending_subscription_request_indices(
        active_subscriptions,
        peer_network_id,
        max_num_active_subscriptions,
        max_num_active_subscriptions * 2,
        num_stream_requests,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_overwrite_streams() {
    // Create test data
    let highest_version = 45576;
    let highest_epoch = 43;
    let lowest_version = 0;
    let peer_version = highest_version - 100;
    let highest_ledger_info =
        utils::create_test_ledger_info_with_sigs(highest_epoch, highest_version);
    let output_list_with_proof =
        utils::create_output_list_with_proof(peer_version + 1, highest_version, highest_version);
    let transaction_list_with_proof = utils::create_transaction_list_with_proof(
        peer_version + 1,
        highest_version,
        highest_version,
        false,
    );

    // Create the mock db reader
    let mut db_reader =
        mock::create_mock_db_for_subscription(highest_ledger_info.clone(), lowest_version);
    utils::expect_get_transaction_outputs(
        &mut db_reader,
        peer_version + 1,
        highest_version - peer_version,
        highest_version,
        output_list_with_proof.clone(),
    );
    utils::expect_get_transactions(
        &mut db_reader,
        peer_version + 1,
        highest_version - peer_version,
        highest_version,
        false,
        transaction_list_with_proof.clone(),
    );

    // Create the storage client and server
    let (mut mock_client, service, mock_time, _) = MockClient::new(Some(db_reader), None);
    let active_subscriptions = service.get_subscriptions();
    tokio::spawn(service.start());

    // Create a peer network ID and stream ID
    let peer_network_id = PeerNetworkId::random();
    let stream_id = utils::get_random_number() as u64;

    // Send multiple requests to subscribe to transaction outputs with the stream ID
    let num_stream_requests = 10;
    let stream_request_indices = utils::create_shuffled_vector(0, num_stream_requests);
    let mut response_receivers = HashMap::new();
    for stream_request_index in stream_request_indices {
        // Send a request to subscribe to transaction outputs
        let response_receiver = utils::subscribe_to_transaction_outputs_for_peer(
            &mut mock_client,
            peer_version,
            highest_epoch,
            stream_id,
            stream_request_index,
            Some(peer_network_id),
        )
        .await;

        // Save the response receiver
        response_receivers.insert(stream_request_index, response_receiver);
    }

    // Wait until the stream requests are active
    utils::wait_for_active_stream_requests(
        active_subscriptions.clone(),
        peer_network_id,
        num_stream_requests as usize,
    )
    .await;

    // Verify no subscription response has been received yet
    utils::verify_no_subscription_responses(&mut response_receivers);

    // Elapse enough time to force the subscription thread to work
    utils::wait_for_subscription_service_to_refresh(&mut mock_client, &mock_time).await;

    // Verify a response is received on the first listener and that it contains the correct data
    let first_response_receiver = response_receivers.remove(&0).unwrap();
    utils::verify_new_transaction_outputs_with_proof(
        &mut mock_client,
        first_response_receiver,
        output_list_with_proof,
        highest_ledger_info.clone(),
    )
    .await;

    // Verify no other subscription response has been received yet
    utils::verify_no_subscription_responses(&mut response_receivers);

    // Send a requests to subscribe to transactions with a new stream ID
    let new_stream_id = utils::get_random_number() as u64;
    let response_receiver = utils::subscribe_to_transactions_for_peer(
        &mut mock_client,
        peer_version,
        highest_epoch,
        false,
        new_stream_id,
        0,
        Some(peer_network_id),
    )
    .await;

    // Wait until the stream requests are active
    utils::wait_for_active_stream_requests(active_subscriptions.clone(), peer_network_id, 1).await;

    // Verify the new stream ID has been used
    utils::verify_active_stream_id_for_peer(
        active_subscriptions.clone(),
        peer_network_id,
        new_stream_id,
    );

    // Elapse enough time to force the subscription thread to work
    utils::wait_for_subscription_service_to_refresh(&mut mock_client, &mock_time).await;

    // Verify a response is received and that it contains the correct data
    utils::verify_new_transactions_with_proof(
        &mut mock_client,
        response_receiver,
        transaction_list_with_proof,
        highest_ledger_info,
    )
    .await;
}

/// Adds a subscription request to the subscription stream for the given peer
fn add_subscription_request_to_stream(
    subscription_request: SubscriptionRequest,
    subscriptions: Arc<Mutex<HashMap<PeerNetworkId, SubscriptionStreamRequests>>>,
    peer_network_id: &PeerNetworkId,
) -> Result<(), (Error, SubscriptionRequest)> {
    let mut subscriptions = subscriptions.lock();
    let subscription_stream_requests = subscriptions.get_mut(peer_network_id).unwrap();
    subscription_stream_requests.add_subscription_request(subscription_request)
}

/// Creates a random request for subscription data
fn create_subscription_data_request(
    known_version: Option<u64>,
    known_epoch: Option<u64>,
    subscription_stream_id: Option<u64>,
    subscription_stream_index: Option<u64>,
) -> DataRequest {
    // Get the request data
    let known_version = known_version.unwrap_or_default();
    let known_epoch = known_epoch.unwrap_or_default();
    let subscription_stream_id = subscription_stream_id.unwrap_or_default();
    let subscription_stream_index = subscription_stream_index.unwrap_or_default();

    // Generate the random data request
    let random_number = utils::get_random_number();
    match random_number % 3 {
        0 => DataRequest::SubscribeTransactionOutputsWithProof(
            SubscribeTransactionOutputsWithProofRequest {
                known_version,
                known_epoch,
                subscription_stream_id,
                subscription_stream_index,
            },
        ),
        1 => DataRequest::SubscribeTransactionsWithProof(SubscribeTransactionsWithProofRequest {
            known_version,
            known_epoch,
            include_events: false,
            subscription_stream_id,
            subscription_stream_index,
        }),
        2 => DataRequest::SubscribeTransactionsOrOutputsWithProof(
            SubscribeTransactionsOrOutputsWithProofRequest {
                known_version,
                known_epoch,
                include_events: false,
                max_num_output_reductions: 0,
                subscription_stream_id,
                subscription_stream_index,
            },
        ),
        number => panic!("This shouldn't be possible! Got: {:?}", number),
    }
}

/// Creates a random subscription request using the given data
fn create_subscription_request(
    time_service: &TimeService,
    known_version: Option<u64>,
    known_epoch: Option<u64>,
    subscription_stream_id: Option<u64>,
    subscription_stream_index: Option<u64>,
) -> SubscriptionRequest {
    // Create a storage service request
    let data_request = create_subscription_data_request(
        known_version,
        known_epoch,
        subscription_stream_id,
        subscription_stream_index,
    );
    let storage_service_request = StorageServiceRequest::new(data_request, true);

    // Create the response sender
    let (callback, _) = oneshot::channel();
    let response_sender = ResponseSender::new(callback);

    // Create a subscription request
    SubscriptionRequest::new(
        storage_service_request,
        response_sender,
        time_service.clone(),
    )
}

/// Creates a random subscription stream using the given data
fn create_subscription_stream_requests(
    time_service: TimeService,
    known_version: Option<u64>,
    known_epoch: Option<u64>,
    subscription_stream_id: Option<u64>,
    subscription_stream_index: Option<u64>,
) -> SubscriptionStreamRequests {
    // Create a new subscription request
    let subscription_request = create_subscription_request(
        &time_service,
        known_version,
        known_epoch,
        subscription_stream_id,
        subscription_stream_index,
    );

    // Create and return the subscription stream containing the request
    SubscriptionStreamRequests::new(subscription_request, time_service)
}

/// Updates the storage server summary with new data and returns
/// the highest synced ledger info.
fn update_storage_summary(
    highest_version: u64,
    highest_epoch: u64,
    cached_storage_server_summary: Arc<RwLock<StorageServerSummary>>,
) -> LedgerInfoWithSignatures {
    // Create a new storage server summary
    let mut storage_server_summary = StorageServerSummary::default();

    // Update the highest synced ledger info
    let synced_ledger_info =
        utils::create_test_ledger_info_with_sigs(highest_epoch, highest_version);
    storage_server_summary.data_summary.synced_ledger_info = Some(synced_ledger_info.clone());

    // Update the epoch ending ledger info range
    storage_server_summary
        .data_summary
        .epoch_ending_ledger_infos = Some(CompleteDataRange::new(0, highest_epoch).unwrap());

    // Update the cached storage server summary
    *cached_storage_server_summary.write() = storage_server_summary;

    // Return the highest synced ledger info
    synced_ledger_info
}

/// Verifies that the pending subscription request indices are valid.
/// Note the expected end indices are exclusive.
fn verify_pending_subscription_request_indices(
    active_subscriptions: Arc<Mutex<HashMap<PeerNetworkId, SubscriptionStreamRequests>>>,
    peer_network_id: PeerNetworkId,
    expected_start_index: u64,
    expected_end_index: u64,
    ignored_end_index: u64,
) {
    // Get the pending subscription requests
    let mut active_subscriptions = active_subscriptions.lock();
    let subscription_stream_requests = active_subscriptions.get_mut(&peer_network_id).unwrap();
    let pending_subscription_requests =
        subscription_stream_requests.get_pending_subscription_requests();

    // Verify that the expected indices are present
    for request_index in expected_start_index..expected_end_index {
        info!(
            "Checking pending subscription request index: {}",
            request_index
        );
        assert!(pending_subscription_requests.contains_key(&request_index));
    }

    // Verify that the ignored indices are not present
    for request_index in expected_end_index..ignored_end_index {
        assert!(!pending_subscription_requests.contains_key(&request_index));
    }
}
