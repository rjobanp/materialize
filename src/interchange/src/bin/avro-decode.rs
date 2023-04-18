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

use std::path::PathBuf;
use std::process;

use anyhow::Context;
use tokio::fs;

use mz_interchange::avro::Decoder;
use mz_interchange::confluent;
use mz_ore::cli;
use mz_ore::cli::CliConfig;
use mz_ore::error::ErrorExt;

/// Decode a single Avro row using Materialize's Avro decoder.
#[derive(clap::Parser)]
struct Args {
    /// The path to a file containing the raw bytes of a single Avro datum.
    data_file: PathBuf,
    /// The path to a file containing the Avro schema.
    schema_file: PathBuf,
    /// Whether the data file uses the Confluent wire format.
    #[clap(long)]
    confluent_wire_format: bool,
}

#[tokio::main]
async fn main() {
    let args: Args = cli::parse_args(CliConfig::default());
    if let Err(e) = run(args).await {
        println!("{}", e.display_with_causes());
        process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    let mut data = &*fs::read(&args.data_file)
        .await
        .context("reading data file")?;
    if args.confluent_wire_format {
        let (schema_id, adjusted_data) = confluent::extract_avro_header(data)?;
        data = adjusted_data;
        println!("schema id: {schema_id}");
    }
    let schema = fs::read_to_string(&args.schema_file)
        .await
        .context("reading schema file")?;
    let ccsr_client: Option<mz_ccsr::Client> = None;
    let debug_name = String::from("avro-decode");
    let confluent_wire_format = false;
    let mut decoder = Decoder::new(&schema, ccsr_client, debug_name, confluent_wire_format)
        .context("creating decoder")?;
    let row = decoder.decode(&mut data).await.context("decoding data")?;
    println!("row: {row:?}");
    Ok(())
}
