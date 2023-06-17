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

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_transactions_or_outputs_different_network() {
    // Test small and large chunk sizes
    let max_output_chunk_size = StorageServiceConfig::default().max_transaction_output_chunk_size;
    for chunk_size in [100, max_output_chunk_size] {
        // Test fallback to transaction syncing
        for fallback_to_transactions in [false, true] {
            // Create test data
            let highest_version = 5060;
            let highest_epoch = 30;
            let lowest_version = 101;
            let peer_version_1 = highest_version - chunk_size;
            let peer_version_2 = highest_version - (chunk_size - 50);
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
            let transaction_list_with_proof = utils::create_transaction_list_with_proof(
                highest_version,
                highest_version,
                highest_version,
                false,
            ); // Creates a small transaction list

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
            if fallback_to_transactions {
                utils::expect_get_transactions(
                    &mut db_reader,
                    peer_version_1 + 1,
                    highest_version - peer_version_1,
                    highest_version,
                    false,
                    transaction_list_with_proof.clone(),
                );
                utils::expect_get_transactions(
                    &mut db_reader,
                    peer_version_2 + 1,
                    highest_version - peer_version_2,
                    highest_version,
                    false,
                    transaction_list_with_proof.clone(),
                );
            }

            // Create the storage client and server
            let storage_config = utils::configure_network_chunk_limit(
                fallback_to_transactions,
                &output_list_with_proof_1,
                &transaction_list_with_proof,
            );
            let (mut mock_client, service, mock_time, _) =
                MockClient::new(Some(db_reader), Some(storage_config));
            let active_subscriptions = service.get_subscriptions();
            tokio::spawn(service.start());

            // Send a request to subscribe to transactions or outputs for peer 1
            let stream_id_1 = 200;
            let stream_index_1 = 0;
            let peer_id = PeerId::random();
            let peer_network_1 = PeerNetworkId::new(NetworkId::Public, peer_id);
            let mut response_receiver_1 = utils::subscribe_to_transactions_or_outputs_for_peer(
                &mut mock_client,
                peer_version_1,
                highest_epoch,
                false,
                0, // Outputs cannot be reduced and will fallback to transactions
                stream_id_1,
                stream_index_1,
                Some(peer_network_1),
            )
            .await;

            // Send a request to subscribe to transactions or outputs for peer 2
            let stream_id_2 = 200;
            let stream_index_2 = 0;
            let peer_network_2 = PeerNetworkId::new(NetworkId::Vfn, peer_id);
            let mut response_receiver_2 = utils::subscribe_to_transactions_or_outputs_for_peer(
                &mut mock_client,
                peer_version_2,
                highest_epoch,
                false,
                0, // Outputs cannot be reduced and will fallback to transactions
                stream_id_2,
                stream_index_2,
                Some(peer_network_2),
            )
            .await;

            // Wait until the subscriptions are active
            utils::wait_for_active_subscriptions(active_subscriptions.clone(), 2).await;

            // Verify no response has been received yet
            assert_none!(response_receiver_1.try_recv().unwrap());
            assert_none!(response_receiver_2.try_recv().unwrap());

            // Elapse enough time to force the subscription service to refresh
            utils::wait_for_subscription_service_to_refresh(&mut mock_client, &mock_time).await;

            // Verify a response is received and that it contains the correct data
            if fallback_to_transactions {
                utils::verify_new_transactions_or_outputs_with_proof(
                    &mut mock_client,
                    response_receiver_1,
                    Some(transaction_list_with_proof.clone()),
                    None,
                    highest_ledger_info.clone(),
                )
                .await;
                utils::verify_new_transactions_or_outputs_with_proof(
                    &mut mock_client,
                    response_receiver_2,
                    Some(transaction_list_with_proof),
                    None,
                    highest_ledger_info,
                )
                .await;
            } else {
                utils::verify_new_transactions_or_outputs_with_proof(
                    &mut mock_client,
                    response_receiver_1,
                    None,
                    Some(output_list_with_proof_1.clone()),
                    highest_ledger_info.clone(),
                )
                .await;
                utils::verify_new_transactions_or_outputs_with_proof(
                    &mut mock_client,
                    response_receiver_2,
                    None,
                    Some(output_list_with_proof_2.clone()),
                    highest_ledger_info,
                )
                .await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_transactions_or_outputs_epoch_change() {
    // Test fallback to transaction syncing
    for fallback_to_transactions in [false, true] {
        // Create test data
        let highest_version = 10000;
        let highest_epoch = 10000;
        let lowest_version = 0;
        let peer_version = highest_version - 1000;
        let peer_epoch = highest_epoch - 1000;
        let epoch_change_version = peer_version + 1;
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
        let transaction_list_with_proof = utils::create_transaction_list_with_proof(
            peer_version + 1,
            peer_version + 1,
            epoch_change_version,
            false,
        ); // Creates a small transaction list

        // Create the mock db reader
        let mut db_reader = mock::create_mock_db_for_subscription(
            utils::create_test_ledger_info_with_sigs(highest_epoch, highest_version),
            lowest_version,
        );
        utils::expect_get_epoch_ending_ledger_infos(
            &mut db_reader,
            peer_epoch,
            peer_epoch + 1,
            epoch_change_proof.clone(),
        );
        utils::expect_get_transaction_outputs(
            &mut db_reader,
            peer_version + 1,
            epoch_change_version - peer_version,
            epoch_change_version,
            output_list_with_proof.clone(),
        );
        if fallback_to_transactions {
            utils::expect_get_transactions(
                &mut db_reader,
                peer_version + 1,
                epoch_change_version - peer_version,
                epoch_change_version,
                false,
                transaction_list_with_proof.clone(),
            );
        }

        // Create the storage client and server
        let storage_config = utils::configure_network_chunk_limit(
            fallback_to_transactions,
            &output_list_with_proof,
            &transaction_list_with_proof,
        );
        let (mut mock_client, service, mock_time, _) =
            MockClient::new(Some(db_reader), Some(storage_config));
        let active_subscriptions = service.get_subscriptions();
        tokio::spawn(service.start());

        // Send a request to subscribe to new transactions or outputs
        let stream_id = 989;
        let stream_index = 0;
        let response_receiver = utils::subscribe_to_transactions_or_outputs(
            &mut mock_client,
            peer_version,
            peer_epoch,
            false,
            5,
            stream_id,
            stream_index,
        )
        .await;

        // Wait until the subscription is active
        utils::wait_for_active_subscriptions(active_subscriptions.clone(), 1).await;

        // Elapse enough time to force the subscription thread to work
        utils::wait_for_subscription_service_to_refresh(&mut mock_client, &mock_time).await;

        // Verify a response is received and that it contains the correct data
        if fallback_to_transactions {
            utils::verify_new_transactions_or_outputs_with_proof(
                &mut mock_client,
                response_receiver,
                Some(transaction_list_with_proof),
                None,
                epoch_change_proof.ledger_info_with_sigs[0].clone(),
            )
            .await;
        } else {
            utils::verify_new_transactions_or_outputs_with_proof(
                &mut mock_client,
                response_receiver,
                None,
                Some(output_list_with_proof),
                epoch_change_proof.ledger_info_with_sigs[0].clone(),
            )
            .await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_transactions_or_outputs_max_chunk() {
    // Test fallback to transaction syncing
    for fallback_to_transactions in [false, true] {
        // Create test data
        let highest_version = 65660;
        let highest_epoch = 30;
        let lowest_version = 101;
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
        let transaction_list_with_proof = utils::create_transaction_list_with_proof(
            peer_version + 1,
            peer_version + 1,
            peer_version + requested_chunk_size,
            false,
        ); // Creates a small transaction list

        // Create the mock db reader
        let max_num_output_reductions = 5;
        let mut db_reader =
            mock::create_mock_db_for_subscription(highest_ledger_info.clone(), lowest_version);
        for i in 0..=max_num_output_reductions {
            utils::expect_get_transaction_outputs(
                &mut db_reader,
                peer_version + 1,
                (max_chunk_size as u32 / (u32::pow(2, i as u32))) as u64,
                highest_version,
                output_list_with_proof.clone(),
            );
        }
        if fallback_to_transactions {
            utils::expect_get_transactions(
                &mut db_reader,
                peer_version + 1,
                max_chunk_size,
                highest_version,
                false,
                transaction_list_with_proof.clone(),
            );
        }

        // Create the storage client and server
        let storage_config = utils::configure_network_chunk_limit(
            fallback_to_transactions,
            &output_list_with_proof,
            &transaction_list_with_proof,
        );
        let (mut mock_client, service, mock_time, _) =
            MockClient::new(Some(db_reader), Some(storage_config));
        let active_subscriptions = service.get_subscriptions();
        tokio::spawn(service.start());

        // Send a request to subscribe to new transactions or outputs
        let stream_id = 545;
        let stream_index = 0;
        let response_receiver = utils::subscribe_to_transactions_or_outputs(
            &mut mock_client,
            peer_version,
            highest_epoch,
            false,
            max_num_output_reductions,
            stream_id,
            stream_index,
        )
        .await;

        // Wait until the subscription is active
        utils::wait_for_active_subscriptions(active_subscriptions.clone(), 1).await;

        // Elapse enough time to force the subscription thread to work
        utils::wait_for_subscription_service_to_refresh(&mut mock_client, &mock_time).await;

        // Verify a response is received and that it contains the correct data
        if fallback_to_transactions {
            utils::verify_new_transactions_or_outputs_with_proof(
                &mut mock_client,
                response_receiver,
                Some(transaction_list_with_proof),
                None,
                highest_ledger_info,
            )
            .await;
        } else {
            utils::verify_new_transactions_or_outputs_with_proof(
                &mut mock_client,
                response_receiver,
                None,
                Some(output_list_with_proof),
                highest_ledger_info,
            )
            .await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_transactions_or_outputs_single_request() {
    // Test small and large chunk sizes
    let storage_service_config = StorageServiceConfig::default();
    let max_output_chunk_size = storage_service_config.max_transaction_output_chunk_size;
    for chunk_size in [1, 100, max_output_chunk_size] {
        // Test fallback to transaction syncing
        for fallback_to_transactions in [false, true] {
            // Create test data
            let highest_version = 5060;
            let highest_epoch = 30;
            let lowest_version = 101;
            let peer_version = highest_version - chunk_size;
            let highest_ledger_info =
                utils::create_test_ledger_info_with_sigs(highest_epoch, highest_version);
            let output_list_with_proof = utils::create_output_list_with_proof(
                peer_version + 1,
                highest_version,
                highest_version,
            );
            let transaction_list_with_proof = utils::create_transaction_list_with_proof(
                highest_version,
                highest_version,
                highest_version,
                false,
            ); // Creates a small transaction list

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
            if fallback_to_transactions {
                utils::expect_get_transactions(
                    &mut db_reader,
                    peer_version + 1,
                    highest_version - peer_version,
                    highest_version,
                    false,
                    transaction_list_with_proof.clone(),
                );
            }

            // Create the storage client and server
            let storage_config = utils::configure_network_chunk_limit(
                fallback_to_transactions,
                &output_list_with_proof,
                &transaction_list_with_proof,
            );
            let (mut mock_client, service, mock_time, _) =
                MockClient::new(Some(db_reader), Some(storage_config));
            let active_subscriptions = service.get_subscriptions();
            tokio::spawn(service.start());

            // Send a request to subscribe to new transactions or outputs
            let stream_id = 0;
            let stream_index = 0;
            let mut response_receiver = utils::subscribe_to_transactions_or_outputs(
                &mut mock_client,
                peer_version,
                highest_epoch,
                false,
                0, // Outputs cannot be reduced and will fallback to transactions
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
            if fallback_to_transactions {
                utils::verify_new_transactions_or_outputs_with_proof(
                    &mut mock_client,
                    response_receiver,
                    Some(transaction_list_with_proof),
                    None,
                    highest_ledger_info,
                )
                .await;
            } else {
                utils::verify_new_transactions_or_outputs_with_proof(
                    &mut mock_client,
                    response_receiver,
                    None,
                    Some(output_list_with_proof),
                    highest_ledger_info,
                )
                .await;
            }

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
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_transaction_or_outputs_streaming() {
    // Test fallback to transaction syncing
    for fallback_to_transactions in [false, true] {
        // Create test data
        let max_transaction_output_chunk_size = 200;
        let num_stream_requests = 30;
        let highest_version = 45576;
        let highest_epoch = 43;
        let lowest_version = 2;
        let peer_version = 1;
        let highest_ledger_info =
            utils::create_test_ledger_info_with_sigs(highest_epoch, highest_version);

        // Create the transaction and output lists with proofs
        let output_lists_with_proofs: Vec<_> = (0..num_stream_requests)
            .map(|i| {
                let start_version = peer_version + (i * max_transaction_output_chunk_size) + 1;
                let end_version = start_version + max_transaction_output_chunk_size - 1;
                utils::create_output_list_with_proof(start_version, end_version, highest_version)
            })
            .collect();
        let transaction_lists_with_proofs: Vec<_> = (0..num_stream_requests)
            .map(|i| {
                let start_version = peer_version + (i * max_transaction_output_chunk_size) + 1;
                let end_version = start_version + max_transaction_output_chunk_size - 1;
                utils::create_transaction_list_with_proof(
                    start_version,
                    end_version,
                    highest_version,
                    false,
                )
            })
            .collect();

        // Create the mock db reader
        let mut db_reader =
            mock::create_mock_db_for_subscription(highest_ledger_info.clone(), lowest_version);
        for i in 0..num_stream_requests {
            let start_version = peer_version + (i * max_transaction_output_chunk_size) + 1;

            // Set expectations for transaction output reads
            utils::expect_get_transaction_outputs(
                &mut db_reader,
                start_version,
                max_transaction_output_chunk_size,
                highest_version,
                output_lists_with_proofs[i as usize].clone(),
            );

            // Set expectations for transaction reads
            if fallback_to_transactions {
                utils::expect_get_transactions(
                    &mut db_reader,
                    start_version,
                    max_transaction_output_chunk_size,
                    highest_version,
                    false,
                    transaction_lists_with_proofs[i as usize].clone(),
                );
            }
        }

        // Create the storage service config
        let mut storage_service_config = utils::configure_network_chunk_limit(
            fallback_to_transactions,
            &output_lists_with_proofs[0],
            &transaction_lists_with_proofs[0],
        );
        storage_service_config.max_transaction_output_chunk_size =
            max_transaction_output_chunk_size;

        // Create the storage client and server
        let (mut mock_client, service, mock_time, _) =
            MockClient::new(Some(db_reader), Some(storage_service_config));
        let active_subscriptions = service.get_subscriptions();
        tokio::spawn(service.start());

        // Create a new peer and stream ID
        let peer_network_id = PeerNetworkId::random();
        let stream_id = utils::get_random_number() as u64;

        // Send all the subscription requests for transactions or outputs
        let stream_request_indices = utils::create_shuffled_vector(0, num_stream_requests);
        let mut response_receivers = HashMap::new();
        for stream_request_index in stream_request_indices {
            // Send the transactions or output subscription request
            let response_receiver = utils::subscribe_to_transactions_or_outputs_for_peer(
                &mut mock_client,
                peer_version,
                highest_epoch,
                false,
                0, // Outputs cannot be reduced and will fallback to transactions
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
            if fallback_to_transactions {
                utils::verify_new_transactions_or_outputs_with_proof(
                    &mut mock_client,
                    response_receiver,
                    Some(transaction_lists_with_proofs[stream_request_index as usize].clone()),
                    None,
                    highest_ledger_info.clone(),
                )
                .await;
            } else {
                utils::verify_new_transactions_or_outputs_with_proof(
                    &mut mock_client,
                    response_receiver,
                    None,
                    Some(output_lists_with_proofs[stream_request_index as usize].clone()),
                    highest_ledger_info.clone(),
                )
                .await;
            }
        }
    }
}
