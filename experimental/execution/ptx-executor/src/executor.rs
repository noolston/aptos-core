// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

//! TODO(aldenhu): doc

use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use crate::scheduler::PtxScheduler;
use aptos_types::transaction::{Transaction, Version};
use rayon::Scope;
use std::sync::mpsc::{channel, Receiver, Sender};
use aptos_state_view::StateView;
use aptos_types::state_store::state_key::StateKey;
use aptos_types::state_store::state_value::StateValue;
use crate::common::{TxnIndex, VersionedKey};

pub(crate) struct PtxExecutor<'a> {
    finalizer: &'a PtxFinalizer,
    work_tx: Sender<Command<'a>>,
    work_rx: Receiver<Command<'a>>,
}

impl PtxExecutor{
    pub fn new(finalizer: &PtxFinalizer) -> Self {
        let (work_tx, work_rx) = channel();
        Self {
            finalizer,
            work_tx,
            work_rx,
        }
    }

    pub fn spawn_work_thread(&self, scope: &Scope<'_>) {
        scope.spawn(|scope| self.work(scope))
    }

    pub fn inform_state_value(&self, key: VersionedKey, value: Option<StateValue>) {
        self.work_tx
            .send(Command::InformStateValue{key, value})
            .expect("Work thread died.");
    }

    pub fn schedule_execute_transaction(&self, transaction: Transaction, dependencies: HashSet<(StateKey, TxnIndex)>) {
        self.work_tx
            .send(Command::AddTransaction {transaction, dependencies})
            .expect("Work thread died.");
    }

    fn work(&self, scope: &Scope<'_>) {
        let mut worker = Worker::new(self.finalizer);
        loop {
            match self.work_rx.recv().expect("Work thread died.") {
                Command::InformStateValue{key, value} => {
                    worker.inform_state_value(key, value);
                }
                Command::AddTransaction {transaction, dependencies} => {
                    worker.add_transaction(transaction, dependencies);
                }
                Command::FinishBlock => {
                    worker.finish_block();
                    break;
                }
            }
        }
    }
}

enum Command {
    InformStateValue {
        key: VersionedKey,
        value: Option<StateValue>
    },
    AddTransaction {
        transaction: Transaction,
        dependencies: HashSet<(StateKey, TxnIndex)>
    },
    FinishBlock,
}

type TxnDependency = HashMap<StateKey, TxnIndex>;

type KeyDependants = HashSet<TxnIndex>;

struct Worker<'a> {
    scope: &'a Scope<'a>,
    finalizer: &'a PtxFinalizer,
    transactions: Vec<Transaction>,
    state_values: HashMap<VersionedKey, Option<StateValue>>,
    key_subscribers: HashMap<VersionedKey, Vec<TxnIndex>>,
    met_dependencies: Vec<HashMap<StateKey, Option<StateValue>>>,
    pending_dependencies: Vec<HashSet<VersionedKey>>,
}

impl<'a> Worker<'a> {
    fn new(scope: &'a Scope<'a>, finalizer: &PtxFinalizer) -> Self {
        Self {
            scope,
            finalizer,
            ..Default::default()
        }
    }

    fn inform_state_value(&mut self, key: VersionedKey, value: Option<StateValue>) {
        assert!(self.state_values.insert(key.clone(), value).is_none());

        if let Some(subscribers) = self.key_subscribers.remove(&key) {
            for txn_index in subscribers {
                self.inform_state_value_to_txn( txn_index, key.clone(), value.clone());
            }
        }
    }

    fn inform_state_value_to_txn(&mut self, txn_index: TxnIndex, key: VersionedKey, value: Option<StateValue>) {
        let pending_dependencies = &mut self.pending_dependencies[txn_index];
        assert!(pending_dependencies.remove(&key), "Pending dependency not found.");
        self.met_dependencies[txn_index].insert(key.0, value);

        if pending_dependencies.is_empty() {
            self.execute_transaction(txn_index);
        }
    }

    fn add_transaction(&mut self, transaction: Transaction, dependencies: HashSet<VersionedKey>) {
        let txn_index = self.transactions.len();
        self.transactions.push(transaction);
        let mut met_dependencies = HashMap::new();
        let mut pending_dependencies = HashSet::new();
        for key in dependencies {
            if let Some(value) = self.state_values.get(&key) {
                met_dependencies.insert(key.0, value.clone());
            } else {
                pending_dependencies.insert(key);
            }
        }
        let ready_to_execute = pending_dependencies.is_empty();

        self.met_dependencies.push(met_dependencies);
        self.pending_dependencies.push(pending_dependencies);

        if ready_to_execute {
            self.execute_transaction(txn_index);
        }
    }

    fn execute_transaction(&self, txn_index: TxnIndex) {
        todo!()
    }
}


// TODO(aldenhu): dedicated worker pool
// TODO(aldenhu): "worker ready" channel
