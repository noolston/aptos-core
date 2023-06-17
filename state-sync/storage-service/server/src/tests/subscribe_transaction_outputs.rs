// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::tests::{mock, mock::MockClient, utils};
use aptos_config::{
    config::StorageServiceConfig,
    network_id::{NetworkId, PeerNetworkId},
};
use aptos_time_service::TimeService;
use aptos_types::{epoch_change::EpochChangeProof, PeerId};
use claims::assert_none;
use std::collections::HashMap;

// Continuous across epoch

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_transaction_outputs_different_networks() {
    // Test small and large chunk sizes
    let max_transaction_output_chunk_size =
        StorageServiceConfig::default().max_transaction_output_chunk_size;
    for chunk_size in [100, max_transaction_output_chunk_size] {
        // Create test data
        let highest_version = 45576;
        let highest_epoch = 43;
        let lowest_version = 4566;
        let peer_version_1 = highest_version - chunk_size;
        let peer_version_2 = highest_version - (chunk_size - 10);
        let highest_ledger_info =
            utils::create_test_ledger_info_with_sigs(highest_epoch, highest_version);
        let output_list_with_proof_1 = utils::create_output_list_with_proof(
            peer_version_1 + 1,
            highest_version,
            highest_version,
        );
        let output_list_with_proof_2 = utils::create_output_list_with_proof(
            peer_version_2 + 1,
            highest_version,
            highest_version,
        );

        // Create the mock db reader
        let mut db_reader =
            mock::create_mock_db_for_subscription(highest_ledger_info.clone(), lowest_version);
        utils::expect_get_transaction_outputs(
            &mut db_reader,
            peer_version_1 + 1,
            highest_version - peer_version_1,
            highest_version,
            output_list_with_proof_1.clone(),
        );
        utils::expect_get_transaction_outputs(
            &mut db_reader,
            peer_version_2 + 1,
            highest_version - peer_version_2,
            highest_version,
            output_list_with_proof_2.clone(),
        );

        // Create the storage client and server
        let (mut mock_client, service, mock_time, _) = MockClient::new(Some(db_reader), None);
        let active_subscriptions = service.get_subscriptions();
        tokio::spawn(service.start());

        // Send a request to subscribe to transaction outputs for peer 1
        let stream_id_1 = 200;
        let stream_index_1 = 0;
        let peer_id = PeerId::random();
        let peer_network_1 = PeerNetworkId::new(NetworkId::Public, peer_id);
        let mut response_receiver_1 = utils::subscribe_to_transaction_outputs_for_peer(
            &mut mock_client,
            peer_version_1,
            highest_epoch,
            stream_id_1,
            stream_index_1,
            Some(peer_network_1),
        )
        .await;

        // Send a request to subscribe to transaction outputs for peer 2
        let stream_id_2 = 200;
        let stream_index_2 = 0;
        let peer_network_2 = PeerNetworkId::new(NetworkId::Vfn, peer_id);
        let mut response_receiver_2 = utils::subscribe_to_transaction_outputs_for_peer(
            &mut mock_client,
            peer_version_2,
            highest_epoch,
            stream_id_2,
            stream_index_2,
            Some(peer_network_2),
        )
        .await;

        // Wait until the subscriptions are active
        utils::wait_for_active_subscriptions(active_subscriptions.clone(), 2).await;

        // Verify no subscription response has been received yet
        assert_none!(response_receiver_1.try_recv().unwrap());
        assert_none!(response_receiver_2.try_recv().unwrap());

        // Elapse enough time to force the subscription thread to work
        utils::wait_for_subscription_service_to_refresh(&mut mock_client, &mock_time).await;

        // Verify a response is received and that it contains the correct data for both peers
        utils::verify_new_transaction_outputs_with_proof(
            &mut mock_client,
            response_receiver_1,
            output_list_with_proof_1,
            highest_ledger_info.clone(),
        )
        .await;
        utils::verify_new_transaction_outputs_with_proof(
            &mut mock_client,
            response_receiver_2,
            output_list_with_proof_2,
            highest_ledger_info,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_transaction_outputs_epoch_change() {
    // Create test data
    let highest_version = 45576;
    let highest_epoch = 1032;
    let lowest_version = 4566;
    let peer_version = highest_version - 100;
    let peer_epoch = highest_epoch - 20;
    let epoch_change_version = peer_version + 45;
    let epoch_change_proof = EpochChangeProof {
        ledger_info_with_sigs: vec![utils::create_test_ledger_info_with_sigs(
            peer_epoch,
            epoch_change_version,
        )],
        more: false,
    };
    let output_list_with_proof = utils::create_output_list_with_proof(
        peer_version + 1,
        epoch_change_version,
        epoch_change_version,
    );

    // Create the mock db reader
    let mut db_reader = mock::create_mock_db_for_subscription(
        utils::create_test_ledger_info_with_sigs(highest_epoch, highest_version),
        lowest_version,
    );
    utils::expect_get_transaction_outputs(
        &mut db_reader,
        peer_version + 1,
        epoch_change_version - peer_version,
        epoch_change_version,
        output_list_with_proof.clone(),
    );
    utils::expect_get_epoch_ending_ledger_infos(
        &mut db_reader,
        peer_epoch,
        peer_epoch + 1,
        epoch_change_proof.clone(),
    );

    // Create the storage client and server
    let (mut mock_client, service, mock_time, _) = MockClient::new(Some(db_reader), None);
    let active_subscriptions = service.get_subscriptions();
    tokio::spawn(service.start());

    // Send a request to subscribe to transaction outputs
    let stream_id = 1;
    let stream_index = 0;
    let response_receiver = utils::subscribe_to_transaction_outputs(
        &mut mock_client,
        peer_version,
        peer_epoch,
        stream_id,
        stream_index,
    )
    .await;

    // Wait until the subscription is active
    utils::wait_for_active_subscriptions(active_subscriptions.clone(), 1).await;

    // Elapse enough time to force the subscription thread to work
    utils::wait_for_subscription_service_to_refresh(&mut mock_client, &mock_time).await;

    // Verify a response is received and that it contains the correct data
    utils::verify_new_transaction_outputs_with_proof(
        &mut mock_client,
        response_receiver,
        output_list_with_proof,
        epoch_change_proof.ledger_info_with_sigs[0].clone(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_transaction_outputs_max_chunk() {
    // Create test data
    let highest_version = 1034556;
    let highest_epoch = 343;
    let lowest_version = 3453;
    let max_chunk_size = StorageServiceConfig::default().max_transaction_output_chunk_size;
    let requested_chunk_size = max_chunk_size + 1;
    let peer_version = highest_version - requested_chunk_size;
    let highest_ledger_info =
        utils::create_test_ledger_info_with_sigs(highest_epoch, highest_version);
    let output_list_with_proof = utils::create_output_list_with_proof(
        peer_version + 1,
        peer_version + requested_chunk_size,
        highest_version,
    );

    // Create the mock db reader
    let mut db_reader =
        mock::create_mock_db_for_subscription(highest_ledger_info.clone(), lowest_version);
    utils::expect_get_transaction_outputs(
        &mut db_reader,
        peer_version + 1,
        max_chunk_size,
        highest_version,
        output_list_with_proof.clone(),
    );

    // Create the storage client and server
    let (mut mock_client, service, mock_time, _) = MockClient::new(Some(db_reader), None);
    let active_subscriptions = service.get_subscriptions();
    tokio::spawn(service.start());

    // Send a request to subscribe to new transaction outputs
    let stream_id = 10561;
    let stream_index = 0;
    let response_receiver = utils::subscribe_to_transaction_outputs(
        &mut mock_client,
        peer_version,
        highest_epoch,
        stream_id,
        stream_index,
    )
    .await;

    // Wait until the subscription is active
    utils::wait_for_active_subscriptions(active_subscriptions.clone(), 1).await;

    // Elapse enough time to force the subscription thread to work
    utils::wait_for_subscription_service_to_refresh(&mut mock_client, &mock_time).await;

    // Verify a response is received and that it contains the correct data
    utils::verify_new_transaction_outputs_with_proof(
        &mut mock_client,
        response_receiver,
        output_list_with_proof,
        highest_ledger_info,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_transaction_outputs_single_request() {
    // Test small and large chunk sizes
    let storage_service_config = StorageServiceConfig::default();
    let max_transaction_output_chunk_size =
        storage_service_config.max_transaction_output_chunk_size;
    for chunk_size in [1, 100, max_transaction_output_chunk_size] {
        // Create test data
        let highest_version = 45576;
        let highest_epoch = 43;
        let lowest_version = 4566;
        let peer_version = highest_version - chunk_size;
        let highest_ledger_info =
            utils::create_test_ledger_info_with_sigs(highest_epoch, highest_version);
        let output_list_with_proof = utils::create_output_list_with_proof(
            peer_version + 1,
            highest_version,
            highest_version,
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

        // Create the storage client and server
        let (mut mock_client, service, mock_time, _) = MockClient::new(Some(db_reader), None);
        let active_subscriptions = service.get_subscriptions();
        tokio::spawn(service.start());

        // Send a request to subscribe to transaction outputs
        let stream_id = 0;
        let stream_index = 0;
        let mut response_receiver = utils::subscribe_to_transaction_outputs(
            &mut mock_client,
            peer_version,
            highest_epoch,
            stream_id,
            stream_index,
        )
        .await;

        // Wait until the subscription is active
        utils::wait_for_active_subscriptions(active_subscriptions.clone(), 1).await;

        // Verify no subscription response has been received yet
        assert_none!(response_receiver.try_recv().unwrap());

        // Elapse enough time to force the subscription thread to work
        utils::wait_for_subscription_service_to_refresh(&mut mock_client, &mock_time).await;

        // Verify a response is received and that it contains the correct data
        utils::verify_new_transaction_outputs_with_proof(
            &mut mock_client,
            response_receiver,
            output_list_with_proof,
            highest_ledger_info,
        )
        .await;

        // Elapse enough time to expire the now empty stream
        utils::elapse_time(
            storage_service_config.max_subscription_period_ms + 1,
            &TimeService::from_mock(mock_time),
        )
        .await;

        // Wait until the subscription is cleared
        utils::wait_for_active_subscriptions(active_subscriptions.clone(), 0).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_transaction_outputs_streaming() {
    // Create a storage service config
    let max_transaction_output_chunk_size = 200;
    let storage_service_config = StorageServiceConfig {
        max_transaction_output_chunk_size,
        ..Default::default()
    };

    // Create test data
    let num_stream_requests = 30;
    let highest_version = 45576;
    let highest_epoch = 43;
    let lowest_version = 4566;
    let peer_version = highest_version - (num_stream_requests * max_transaction_output_chunk_size);
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
    for i in 0..num_stream_requests {
        utils::expect_get_transaction_outputs(
            &mut db_reader,
            peer_version + (i * max_transaction_output_chunk_size) + 1,
            max_transaction_output_chunk_size,
            highest_version,
            output_lists_with_proofs[i as usize].clone(),
        );
    }

    // Create the storage client and server
    let (mut mock_client, service, mock_time, _) =
        MockClient::new(Some(db_reader), Some(storage_service_config));
    let active_subscriptions = service.get_subscriptions();
    tokio::spawn(service.start());

    // Create a new peer and stream ID
    let peer_network_id = PeerNetworkId::random();
    let stream_id = utils::get_random_number() as u64;

    // Send all the subscription requests for transaction outputs
    let stream_request_indices = utils::create_shuffled_vector(0, num_stream_requests);
    let mut response_receivers = HashMap::new();
    for stream_request_index in stream_request_indices {
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

    // Wait until the stream requests are active
    utils::wait_for_active_stream_requests(
        active_subscriptions.clone(),
        peer_network_id,
        num_stream_requests as usize,
    )
    .await;

    // Continuously run the subscription service until all the responses are received
    for stream_request_index in 0..num_stream_requests {
        // Verify the state of the subscription stream
        utils::verify_subscription_stream_state(
            active_subscriptions.clone(),
            peer_network_id,
            num_stream_requests,
            peer_version,
            highest_epoch,
            max_transaction_output_chunk_size,
        );

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
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_transaction_outputs_streaming_epoch_change() {
    // Create a storage service config
    let max_transaction_output_chunk_size = 5;
    let storage_service_config = StorageServiceConfig {
        max_transaction_output_chunk_size,
        ..Default::default()
    };

    // Create test data
    let num_stream_requests = 50;
    let highest_version = 1000;
    let highest_epoch = 2;
    let lowest_version = 0;
    let peer_version = highest_version - 500;
    let peer_epoch = highest_epoch - 1;
    let epoch_change_version = peer_version + 97;

    // Create the highest ledger info and epoch change proof
    let highest_ledger_info =
        utils::create_test_ledger_info_with_sigs(highest_epoch, highest_version);
    let epoch_change_ledger_info =
        utils::create_epoch_ending_ledger_info(peer_epoch, epoch_change_version);
    let epoch_change_proof = EpochChangeProof {
        ledger_info_with_sigs: vec![epoch_change_ledger_info.clone()],
        more: false,
    };

    // Create the output lists with proofs
    let start_and_end_versions: Vec<_> = (0..num_stream_requests)
        .map(|i| {
            let start_version = peer_version + (i * max_transaction_output_chunk_size) + 1;
            let end_version = start_version + max_transaction_output_chunk_size - 1;
            if end_version < epoch_change_version {
                (start_version, end_version) // The chunk is before the epoch change
            } else if start_version < epoch_change_version && epoch_change_version < end_version {
                (start_version, epoch_change_version) // The chunk would cross the epoch boundary
            } else {
                let chunk_shift_amount =
                    (start_version - epoch_change_version - 1) % max_transaction_output_chunk_size;
                (
                    start_version - chunk_shift_amount,
                    end_version - chunk_shift_amount,
                ) // The chunk is after the epoch change (shift it left)
            }
        })
        .collect();
    let output_lists_with_proofs: Vec<_> = start_and_end_versions
        .iter()
        .map(|(start_version, end_version)| {
            utils::create_output_list_with_proof(*start_version, *end_version, highest_version)
        })
        .collect();

    // Create the mock db reader
    let mut db_reader =
        mock::create_mock_db_for_subscription(highest_ledger_info.clone(), lowest_version);
    utils::expect_get_epoch_ending_ledger_infos(
        &mut db_reader,
        peer_epoch,
        peer_epoch + 1,
        epoch_change_proof.clone(),
    );
    for (i, (start_version, end_version)) in start_and_end_versions.iter().enumerate() {
        let proof_version = if *end_version <= epoch_change_version {
            epoch_change_version
        } else {
            highest_version
        };
        utils::expect_get_transaction_outputs(
            &mut db_reader,
            *start_version,
            end_version - start_version + 1,
            proof_version,
            output_lists_with_proofs[i].clone(),
        );
    }

    // Create the storage client and server
    let (mut mock_client, service, mock_time, _) =
        MockClient::new(Some(db_reader), Some(storage_service_config));
    let active_subscriptions = service.get_subscriptions();
    tokio::spawn(service.start());

    // Create a new peer and stream ID
    let peer_network_id = PeerNetworkId::random();
    let stream_id = utils::get_random_number() as u64;

    // Send all the subscription requests for transaction outputs
    let stream_request_indices = utils::create_shuffled_vector(0, num_stream_requests);
    let mut response_receivers = HashMap::new();
    for stream_request_index in stream_request_indices {
        // Send the transaction output subscription request
        let response_receiver = utils::subscribe_to_transaction_outputs_for_peer(
            &mut mock_client,
            peer_version,
            peer_epoch,
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

    // Continuously run the subscription service until all the responses are received
    for stream_request_index in 0..num_stream_requests {
        // Elapse enough time to force the subscription thread to work
        utils::wait_for_subscription_service_to_refresh(&mut mock_client, &mock_time).await;

        // Verify a response is received and that it contains the correct data
        let response_receiver = response_receivers.remove(&stream_request_index).unwrap();
        let expected_output_list_with_proof =
            output_lists_with_proofs[stream_request_index as usize].clone();
        let expected_ledger_info = if expected_output_list_with_proof
            .first_transaction_output_version
            .unwrap()
            > epoch_change_version
        {
            highest_ledger_info.clone()
        } else {
            epoch_change_ledger_info.clone()
        };
        utils::verify_new_transaction_outputs_with_proof(
            &mut mock_client,
            response_receiver,
            output_lists_with_proofs[stream_request_index as usize].clone(),
            expected_ledger_info,
        )
        .await;
    }
}
