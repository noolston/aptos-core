// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::AptosDB;
use anyhow::{anyhow, format_err, Result};
use aptos_config::config::NodeConfig;
use aptos_crypto::HashValue;
use aptos_storage_interface::{DbReader, DbReaderWriter, ExecutedTrees, Order};
use aptos_types::{
    account_config::NewBlockEvent,
    contract_event::{ContractEvent, EventWithVersion},
    epoch_change::EpochChangeProof,
    epoch_state::EpochState,
    event::EventKey,
    ledger_info::LedgerInfoWithSignatures,
    proof::{
        AccumulatorConsistencyProof, SparseMerkleProof, SparseMerkleProofExt,
        TransactionAccumulatorRangeProof, TransactionAccumulatorSummary,
    },
    state_proof::StateProof,
    state_store::{
        state_key::StateKey,
        state_key_prefix::StateKeyPrefix,
        state_storage_usage::StateStorageUsage,
        state_value::{StateValue, StateValueChunkWithProof},
        table::{TableHandle, TableInfo},
    },
    transaction::{
        AccountTransactionsWithProof, Transaction, TransactionInfo, TransactionListWithProof,
        TransactionOutputListWithProof, TransactionWithProof, Version,
    },
    write_set::WriteSet,
};
use move_core_types::account_address::AccountAddress;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// This is a wrapper around [AptosDB] that is used to bootstrap the node for fast sync mode
/// fast_sync_db is used for accessing genesis data
pub struct FastSyncAptosDB {
    // refer to the regular DB
    inner: Arc<dyn DbReader>,
    //TODO(bowu): remove this hack when we have a better way to handle fast sync
    // storing the genesis data
    fast_sync_db: Option<DbReaderWriter>,
    fast_sync_finished: Arc<AtomicBool>,
}

impl FastSyncAptosDB {
    pub fn new_fast_sync_aptos_db(db: Arc<dyn DbReader>, config: &NodeConfig) -> Result<Self> {
        // append temp to the end of the db dir
        let mut db_dir = config.storage.dir();
        db_dir.push("fast_sync_temp_db");
        let fast_sync_db = AptosDB::open(
            db_dir.as_path(),
            false,
            config.storage.storage_pruner_config,
            config.storage.rocksdb_configs,
            config.storage.enable_indexer,
            config.storage.buffered_state_target_items,
            config.storage.max_num_nodes_per_lru_cache_shard,
        )
        .map_err(|err| anyhow!("fast sync DB failed to open {}", err))?;
        Ok(Self {
            inner: db,
            fast_sync_db: Some(DbReaderWriter::new(fast_sync_db)),
            fast_sync_finished: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Expose the fast sync db for committing genesis data
    pub fn get_db_reader_writer(&mut self) -> Option<&mut DbReaderWriter> {
        self.fast_sync_db.as_mut()
    }

    /// Check if the fast sync finished already
    fn is_fast_sync_bootstrap_finished(&self) -> bool {
        if self.fast_sync_finished.load(Ordering::SeqCst) {
            return true;
        }
        let li_res = self.inner.get_latest_ledger_info();
        li_res
            .map(|li| {
                // ledger info is only updated when fast sync is finished
                let finished = li.ledger_info().version() > 0;
                if finished {
                    self.fast_sync_finished.store(true, Ordering::SeqCst)
                }
                finished
            })
            .unwrap_or(false)
    }
}

impl DbReader for FastSyncAptosDB {
    fn get_epoch_ending_ledger_infos(
        &self,
        start_epoch: u64,
        end_epoch: u64,
    ) -> Result<EpochChangeProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_epoch_ending_ledger_infos(start_epoch, end_epoch)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader
                        .get_epoch_ending_ledger_infos(start_epoch, end_epoch)
                })
        }
    }

    fn get_transactions(
        &self,
        start_version: Version,
        batch_size: u64,
        ledger_version: Version,
        fetch_events: bool,
    ) -> Result<TransactionListWithProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_transactions(start_version, batch_size, ledger_version, fetch_events)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader.get_transactions(
                        start_version,
                        batch_size,
                        ledger_version,
                        fetch_events,
                    )
                })
        }
    }

    fn get_transaction_by_hash(
        &self,
        hash: HashValue,
        ledger_version: Version,
        fetch_events: bool,
    ) -> Result<Option<TransactionWithProof>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_transaction_by_hash(hash, ledger_version, fetch_events)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader
                        .get_transaction_by_hash(hash, ledger_version, fetch_events)
                })
        }
    }

    fn get_transaction_by_version(
        &self,
        version: Version,
        ledger_version: Version,
        fetch_events: bool,
    ) -> Result<TransactionWithProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_transaction_by_version(version, ledger_version, fetch_events)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader
                        .get_transaction_by_version(version, ledger_version, fetch_events)
                })
        }
    }

    fn get_first_txn_version(&self) -> Result<Option<Version>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_first_txn_version()
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_first_txn_version())
        }
    }

    fn get_first_viable_txn_version(&self) -> Result<Version> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_first_viable_txn_version()
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_first_viable_txn_version())
        }
    }

    fn get_first_write_set_version(&self) -> Result<Option<Version>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_first_write_set_version()
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_first_write_set_version())
        }
    }

    /// See [AptosDB::get_transaction_outputs].
    ///
    /// [AptosDB::get_transaction_outputs]: ../aptosdb/struct.AptosDB.html#method.get_transaction_outputs
    fn get_transaction_outputs(
        &self,
        start_version: Version,
        limit: u64,
        ledger_version: Version,
    ) -> Result<TransactionOutputListWithProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_transaction_outputs(start_version, limit, ledger_version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader
                        .get_transaction_outputs(start_version, limit, ledger_version)
                })
        }
    }

    /// Returns events by given event key
    fn get_events(
        &self,
        event_key: &EventKey,
        start: u64,
        order: Order,
        limit: u64,
        ledger_version: Version,
    ) -> Result<Vec<EventWithVersion>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_events(event_key, start, order, limit, ledger_version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader
                        .get_events(event_key, start, order, limit, ledger_version)
                })
        }
    }

    fn get_transaction_iterator(
        &self,
        start_version: Version,
        limit: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<Transaction>> + '_>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_transaction_iterator(start_version, limit)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_transaction_iterator(start_version, limit))
        }
    }

    fn get_transaction_info_iterator(
        &self,
        start_version: Version,
        limit: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<TransactionInfo>> + '_>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_transaction_info_iterator(start_version, limit)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader
                        .get_transaction_info_iterator(start_version, limit)
                })
        }
    }

    fn get_events_iterator(
        &self,
        start_version: Version,
        limit: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<Vec<ContractEvent>>> + '_>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_events_iterator(start_version, limit)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_events_iterator(start_version, limit))
        }
    }

    fn get_write_set_iterator(
        &self,
        start_version: Version,
        limit: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<WriteSet>> + '_>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_write_set_iterator(start_version, limit)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_write_set_iterator(start_version, limit))
        }
    }

    fn get_transaction_accumulator_range_proof(
        &self,
        start_version: Version,
        limit: u64,
        ledger_version: Version,
    ) -> Result<TransactionAccumulatorRangeProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_transaction_accumulator_range_proof(start_version, limit, ledger_version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader.get_transaction_accumulator_range_proof(
                        start_version,
                        limit,
                        ledger_version,
                    )
                })
        }
    }

    /// See [AptosDB::get_block_timestamp].
    ///
    /// [AptosDB::get_block_timestamp]:
    /// ../aptosdb/struct.AptosDB.html#method.get_block_timestamp
    fn get_block_timestamp(&self, version: Version) -> Result<u64> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_block_timestamp(version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_block_timestamp(version))
        }
    }

    fn get_next_block_event(&self, version: Version) -> Result<(Version, NewBlockEvent)> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_next_block_event(version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_next_block_event(version))
        }
    }

    /// Returns the start_version, end_version and NewBlockEvent of the block containing the input
    /// transaction version.
    fn get_block_info_by_version(
        &self,
        version: Version,
    ) -> Result<(Version, Version, NewBlockEvent)> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_block_info_by_version(version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_block_info_by_version(version))
        }
    }

    /// Returns the start_version, end_version and NewBlockEvent of the block containing the input
    /// transaction version.
    fn get_block_info_by_height(&self, height: u64) -> Result<(Version, Version, NewBlockEvent)> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_block_info_by_height(height)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_block_info_by_height(height))
        }
    }

    /// Gets the version of the last transaction committed before timestamp,
    /// a committed block at or after the required timestamp must exist (otherwise it's possible
    /// the next block committed as a timestamp smaller than the one in the request).
    fn get_last_version_before_timestamp(
        &self,
        _timestamp: u64,
        _ledger_version: Version,
    ) -> Result<Version> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_last_version_before_timestamp(_timestamp, _ledger_version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader
                        .get_last_version_before_timestamp(_timestamp, _ledger_version)
                })
        }
    }

    /// Gets the latest epoch state currently held in storage.
    fn get_latest_epoch_state(&self) -> Result<EpochState> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_latest_epoch_state()
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_latest_epoch_state())
        }
    }

    /// Returns the (key, value) iterator for a particular state key prefix at at desired version. This
    /// API can be used to get all resources of an account by passing the account address as the
    /// key prefix.
    fn get_prefixed_state_value_iterator(
        &self,
        key_prefix: &StateKeyPrefix,
        cursor: Option<&StateKey>,
        version: Version,
    ) -> Result<Box<dyn Iterator<Item = Result<(StateKey, StateValue)>> + '_>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_prefixed_state_value_iterator(key_prefix, cursor, version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader
                        .get_prefixed_state_value_iterator(key_prefix, cursor, version)
                })
        }
    }

    /// Returns the latest ledger info, if any.
    fn get_latest_ledger_info_option(&self) -> Result<Option<LedgerInfoWithSignatures>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_latest_ledger_info_option()
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_latest_ledger_info_option())
        }
    }

    /// Returns the latest ledger info.
    fn get_latest_ledger_info(&self) -> Result<LedgerInfoWithSignatures> {
        self.get_latest_ledger_info_option()
            .and_then(|opt| opt.ok_or_else(|| format_err!("Latest LedgerInfo not found.")))
    }

    /// Returns the latest committed version, error on on non-bootstrapped/empty DB.
    fn get_latest_version(&self) -> Result<Version> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_latest_version()
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_latest_version())
        }
    }

    /// Returns the latest state checkpoint version if any.
    fn get_latest_state_checkpoint_version(&self) -> Result<Option<Version>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_latest_state_checkpoint_version()
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_latest_state_checkpoint_version())
        }
    }

    /// Returns the latest state snapshot strictly before `next_version` if any.
    fn get_state_snapshot_before(
        &self,
        next_version: Version,
    ) -> Result<Option<(Version, HashValue)>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_state_snapshot_before(next_version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_state_snapshot_before(next_version))
        }
    }

    /// Returns the latest version and committed block timestamp
    fn get_latest_commit_metadata(&self) -> Result<(Version, u64)> {
        let ledger_info_with_sig = self.get_latest_ledger_info()?;
        let ledger_info = ledger_info_with_sig.ledger_info();
        Ok((ledger_info.version(), ledger_info.timestamp_usecs()))
    }

    /// Returns a transaction that is the `seq_num`-th one associated with the given account. If
    /// the transaction with given `seq_num` doesn't exist, returns `None`.
    fn get_account_transaction(
        &self,
        address: AccountAddress,
        seq_num: u64,
        include_events: bool,
        ledger_version: Version,
    ) -> Result<Option<TransactionWithProof>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_account_transaction(address, seq_num, include_events, ledger_version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader.get_account_transaction(
                        address,
                        seq_num,
                        include_events,
                        ledger_version,
                    )
                })
        }
    }

    /// Returns the list of transactions sent by an account with `address` starting
    /// at sequence number `seq_num`. Will return no more than `limit` transactions.
    /// Will ignore transactions with `txn.version > ledger_version`. Optionally
    /// fetch events for each transaction when `fetch_events` is `true`.
    fn get_account_transactions(
        &self,
        address: AccountAddress,
        seq_num: u64,
        limit: u64,
        include_events: bool,
        ledger_version: Version,
    ) -> Result<AccountTransactionsWithProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_account_transactions(
                address,
                seq_num,
                limit,
                include_events,
                ledger_version,
            )
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader.get_account_transactions(
                        address,
                        seq_num,
                        limit,
                        include_events,
                        ledger_version,
                    )
                })
        }
    }

    /// Returns proof of new state for a given ledger info with signatures relative to version known
    /// to client
    fn get_state_proof_with_ledger_info(
        &self,
        known_version: u64,
        ledger_info: LedgerInfoWithSignatures,
    ) -> Result<StateProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_state_proof_with_ledger_info(known_version, ledger_info)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader
                        .get_state_proof_with_ledger_info(known_version, ledger_info)
                })
        }
    }

    /// Returns proof of new state relative to version known to client
    fn get_state_proof(&self, known_version: u64) -> Result<StateProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_state_proof(known_version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_state_proof(known_version))
        }
    }

    /// Gets the state value by state key at version.
    /// See [AptosDB::get_state_value_by_version].
    ///
    /// [AptosDB::get_state_value_by_version]:
    /// ../aptosdb/struct.AptosDB.html#method.get_state_value_by_version
    fn get_state_value_by_version(
        &self,
        state_key: &StateKey,
        version: Version,
    ) -> Result<Option<StateValue>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_state_value_by_version(state_key, version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_state_value_by_version(state_key, version))
        }
    }

    /// Get the latest state value and its corresponding version when it's of the given key up
    /// to the given version.
    /// See [AptosDB::get_state_value_with_version_by_version].
    ///
    /// [AptosDB::get_state_value_with_version_by_version]:
    /// ../aptosdb/struct.AptosDB.html#method.get_state_value_with_version_by_version
    fn get_state_value_with_version_by_version(
        &self,
        state_key: &StateKey,
        version: Version,
    ) -> Result<Option<(Version, StateValue)>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_state_value_with_version_by_version(state_key, version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader
                        .get_state_value_with_version_by_version(state_key, version)
                })
        }
    }

    /// Returns the proof of the given state key and version.
    fn get_state_proof_by_version_ext(
        &self,
        state_key: &StateKey,
        version: Version,
    ) -> Result<SparseMerkleProofExt> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_state_proof_by_version_ext(state_key, version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_state_proof_by_version_ext(state_key, version))
        }
    }

    /// Gets a state value by state key along with the proof, out of the ledger state indicated by the state
    /// Merkle tree root with a sparse merkle proof proving state tree root.
    /// See [AptosDB::get_account_state_with_proof_by_version].
    ///
    /// [AptosDB::get_account_state_with_proof_by_version]:
    /// ../aptosdb/struct.AptosDB.html#method.get_account_state_with_proof_by_version
    ///
    /// This is used by aptos core (executor) internally.
    fn get_state_value_with_proof_by_version_ext(
        &self,
        state_key: &StateKey,
        version: Version,
    ) -> Result<(Option<StateValue>, SparseMerkleProofExt)> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_state_value_with_proof_by_version_ext(state_key, version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader
                        .get_state_value_with_proof_by_version_ext(state_key, version)
                })
        }
    }

    fn get_state_value_with_proof_by_version(
        &self,
        state_key: &StateKey,
        version: Version,
    ) -> Result<(Option<StateValue>, SparseMerkleProof)> {
        self.get_state_value_with_proof_by_version_ext(state_key, version)
            .map(|(value, proof_ext)| (value, proof_ext.into()))
    }

    /// Gets the latest ExecutedTrees no matter if db has been bootstrapped.
    /// Used by the Db-bootstrapper.
    fn get_latest_executed_trees(&self) -> Result<ExecutedTrees> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_latest_executed_trees()
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_latest_executed_trees())
        }
    }

    /// Get the ledger info of the epoch that `known_version` belongs to.
    fn get_epoch_ending_ledger_info(&self, known_version: u64) -> Result<LedgerInfoWithSignatures> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_epoch_ending_ledger_info(known_version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_epoch_ending_ledger_info(known_version))
        }
    }

    /// Gets the transaction accumulator root hash at specified version.
    /// Caller must guarantee the version is not greater than the latest version.
    fn get_accumulator_root_hash(&self, _version: Version) -> Result<HashValue> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_accumulator_root_hash(_version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_accumulator_root_hash(_version))
        }
    }

    /// Gets an [`AccumulatorConsistencyProof`] starting from `client_known_version`
    /// (or pre-genesis if `None`) until `ledger_version`.
    ///
    /// In other words, if the client has an accumulator summary for
    /// `client_known_version`, they can use the result from this API to efficiently
    /// extend their accumulator to `ledger_version` and prove that the new accumulator
    /// is consistent with their old accumulator. By consistent, we mean that by
    /// appending the actual `ledger_version - client_known_version` transactions
    /// to the old accumulator summary you get the new accumulator summary.
    ///
    /// If the client is starting up for the first time and has no accumulator
    /// summary yet, they can call this with `client_known_version=None`, i.e.,
    /// pre-genesis, to get the complete accumulator summary up to `ledger_version`.
    fn get_accumulator_consistency_proof(
        &self,
        _client_known_version: Option<Version>,
        _ledger_version: Version,
    ) -> Result<AccumulatorConsistencyProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_accumulator_consistency_proof(_client_known_version, _ledger_version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader
                        .get_accumulator_consistency_proof(_client_known_version, _ledger_version)
                })
        }
    }

    /// A convenience function for building a [`TransactionAccumulatorSummary`]
    /// at the given `ledger_version`.
    ///
    /// Note: this is roughly equivalent to calling
    /// `DbReader::get_accumulator_consistency_proof(None, ledger_version)`.
    fn get_accumulator_summary(
        &self,
        ledger_version: Version,
    ) -> Result<TransactionAccumulatorSummary> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_accumulator_summary(ledger_version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_accumulator_summary(ledger_version))
        }
    }

    /// Returns total number of leaves in state store at given version.
    fn get_state_leaf_count(&self, version: Version) -> Result<usize> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_state_leaf_count(version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_state_leaf_count(version))
        }
    }

    /// Get a chunk of state store value, addressed by the index.
    fn get_state_value_chunk_with_proof(
        &self,
        version: Version,
        start_idx: usize,
        chunk_size: usize,
    ) -> Result<StateValueChunkWithProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner
                .get_state_value_chunk_with_proof(version, start_idx, chunk_size)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| {
                    db.reader
                        .get_state_value_chunk_with_proof(version, start_idx, chunk_size)
                })
        }
    }

    /// Returns if the state store pruner is enabled.
    fn is_state_merkle_pruner_enabled(&self) -> Result<bool> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.is_state_merkle_pruner_enabled()
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.is_state_merkle_pruner_enabled())
        }
    }

    /// Get the state prune window config value.
    fn get_epoch_snapshot_prune_window(&self) -> Result<usize> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_epoch_snapshot_prune_window()
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_epoch_snapshot_prune_window())
        }
    }

    /// Returns if the ledger pruner is enabled.
    fn is_ledger_pruner_enabled(&self) -> Result<bool> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.is_ledger_pruner_enabled()
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.is_ledger_pruner_enabled())
        }
    }

    /// Get the ledger prune window config value.
    fn get_ledger_prune_window(&self) -> Result<usize> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_ledger_prune_window()
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_ledger_prune_window())
        }
    }

    /// Get table info from the internal indexer.
    fn get_table_info(&self, handle: TableHandle) -> Result<TableInfo> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_table_info(handle)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_table_info(handle))
        }
    }

    /// Returns whether the internal indexer DB has been enabled or not
    fn indexer_enabled(&self) -> bool {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.indexer_enabled()
        } else {
            self.fast_sync_db
                .as_ref()
                .map(|db| db.reader.indexer_enabled())
                .unwrap_or(false)
        }
    }

    /// Returns state storage usage at the end of an epoch.
    fn get_state_storage_usage(&self, version: Option<Version>) -> Result<StateStorageUsage> {
        if self.is_fast_sync_bootstrap_finished() {
            self.inner.get_state_storage_usage(version)
        } else {
            self.fast_sync_db
                .as_ref()
                .ok_or_else(|| anyhow!("fast sync db is not initialized"))
                .and_then(|db| db.reader.get_state_storage_usage(version))
        }
    }
}
