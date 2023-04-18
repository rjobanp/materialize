// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

//! Segment library for Rust.
//!
//! This crate provides a library to the [Segment] analytics platform.
//! It is a small wrapper around the [`segment`] crate to provide a more
//! ergonomic interface.
//!
//! [Segment]: https://segment.com
//! [`segment`]: https://docs.rs/segment

use segment::message::{Batch, BatchMessage, Group, Message, Track, User};
use segment::{Batcher, Client as _, HttpClient};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::warn;
use uuid::Uuid;

/// The maximum number of undelivered events. Once this limit is reached,
/// new events will be dropped.
const MAX_PENDING_EVENTS: usize = 32_768;

/// A [Segment] API client.
///
/// Event delivery is best effort. There is no guarantee that a given event
/// will be delivered to Segment.
///
/// [Segment]: https://segment.com
#[derive(Clone)]
pub struct Client {
    tx: Sender<BatchMessage>,
}

impl Client {
    /// Creates a new client.
    pub fn new(api_key: String) -> Client {
        let (tx, rx) = mpsc::channel(MAX_PENDING_EVENTS);

        let send_task = SendTask {
            api_key,
            http_client: HttpClient::default(),
        };
        mz_ore::task::spawn(
            || "segment_send_task",
            async move { send_task.run(rx).await },
        );

        Client { tx }
    }

    /// Sends a new [track event] to Segment.
    ///
    /// Delivery happens asynchronously on a background thread. It is best
    /// effort. There is no guarantee that the event will be delivered to
    /// Segment. Events may be dropped when the client is backlogged. Errors are
    /// logged but not returned.
    ///
    /// [track event]: https://segment.com/docs/connections/spec/track/
    pub fn track<S>(
        &self,
        user_id: Uuid,
        event: S,
        properties: serde_json::Value,
        context: Option<serde_json::Value>,
    ) where
        S: Into<String>,
    {
        self.send(BatchMessage::Track(Track {
            user: User::UserId {
                user_id: user_id.to_string(),
            },
            event: event.into(),
            properties,
            context,
            ..Default::default()
        }));
    }

    /// Sends a new [group event] to Segment.
    ///
    /// Delivery happens asynchronously on a background thread. It is best
    /// effort. There is no guarantee that the event will be delivered to
    /// Segment. Events may be dropped when the client is backlogged. Errors are
    /// logged but not returned.
    ///
    /// [track event]: https://segment.com/docs/connections/spec/group/
    pub fn group(&self, user_id: Uuid, group_id: Uuid, traits: serde_json::Value) {
        self.send(BatchMessage::Group(Group {
            user: User::UserId {
                user_id: user_id.to_string(),
            },
            group_id: group_id.to_string(),
            traits,
            ..Default::default()
        }));
    }

    fn send(&self, message: BatchMessage) {
        match self.tx.try_send(message) {
            Ok(()) => (),
            Err(TrySendError::Closed(_)) => panic!("receiver must not drop first"),
            Err(TrySendError::Full(_)) => {
                warn!("dropping segment event because queue is full");
            }
        }
    }
}

struct SendTask {
    api_key: String,
    http_client: HttpClient,
}

impl SendTask {
    async fn run(&self, mut rx: Receiver<BatchMessage>) {
        // On each turn of the loop, we accumulate all outstanding messages and
        // send them to Segment in the largest batches possible. We never have
        // more than one outstanding request to Segment.
        loop {
            let mut batcher = Batcher::new(None);

            // Wait for the first event to arrive.
            match rx.recv().await {
                Some(message) => batcher = self.enqueue(batcher, message).await,
                None => return,
            };

            // Accumulate any other messages that are ready. `enqueue` may
            // flush the batch to Segment if we hit the maximum batch size.
            while let Ok(message) = rx.try_recv() {
                batcher = self.enqueue(batcher, message).await;
            }

            // Drain the queue.
            self.flush(batcher).await;
        }
    }

    async fn enqueue(&self, mut batcher: Batcher, message: BatchMessage) -> Batcher {
        match batcher.push(message) {
            Ok(None) => (),
            Ok(Some(message)) => {
                self.flush(batcher).await;
                batcher = Batcher::new(None);
                batcher
                    .push(message)
                    .expect("message cannot fail to enqueue twice");
            }
            Err(e) => {
                warn!("error enqueueing segment message: {}", e);
            }
        }
        batcher
    }

    async fn flush(&self, batcher: Batcher) {
        let message = batcher.into_message();
        if matches!(&message, Message::Batch(Batch { batch , .. }) if batch.is_empty()) {
            return;
        }
        if let Err(e) = self.http_client.send(self.api_key.clone(), message).await {
            warn!("error sending message to segment: {}", e);
        }
    }
}
