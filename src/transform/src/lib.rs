// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations for relation expressions.
//!
//! This crate contains traits, types, and methods suitable for transforming
//! `MirRelationExpr` types in ways that preserve semantics and improve performance.
//! The core trait is `Transform`, and many implementors of this trait can be
//! boxed and iterated over. Some common transformation patterns are wrapped
//! as `Transform` implementors themselves.
//!
//! The crate also contains the beginnings of whole-dataflow optimization,
//! which uses the same analyses but spanning multiple dataflow elements.

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use std::error::Error;
use std::sync::Arc;
use std::{fmt, iter};

use mz_expr::visit::Visit;
use mz_expr::{MirRelationExpr, MirScalarExpr};
use mz_ore::id_gen::IdGen;
use mz_ore::stack::RecursionLimitError;
use mz_repr::GlobalId;
use tracing::error;

pub mod attribute;
pub mod canonicalization;
pub mod canonicalize_mfp;
pub mod column_knowledge;
pub mod compound;
pub mod cse;
pub mod dataflow;
pub mod demand;
pub mod fold_constants;
pub mod fusion;
pub mod join_implementation;
pub mod literal_constraints;
pub mod literal_lifting;
pub mod monotonic;
pub mod movement;
pub mod non_null_requirements;
pub mod nonnullable;
pub mod normalize_lets;
pub mod normalize_ops;
pub mod notice;
pub mod ordering;
pub mod predicate_pushdown;
pub mod reduce_elision;
pub mod reduction_pushdown;
pub mod redundant_join;
pub mod semijoin_idempotence;
pub mod symbolic;
pub mod threshold_elision;
pub mod typecheck;
pub mod union_cancel;

use crate::dataflow::DataflowMetainfo;
pub use dataflow::optimize_dataflow;
use mz_ore::soft_assert_or_log;

/// Compute the conjunction of a variadic number of expressions.
#[macro_export]
macro_rules! all {
    ($x:expr) => ($x);
    ($($x:expr,)+) => ( $($x)&&+ )
}

/// Compute the disjunction of a variadic number of expressions.
#[macro_export]
macro_rules! any {
    ($x:expr) => ($x);
    ($($x:expr,)+) => ( $($x)||+ )
}

/// Arguments that get threaded through all transforms, plus a `DataflowMetainfo` that can be
/// manipulated by the transforms.
#[derive(Debug)]
pub struct TransformCtx<'a> {
    /// The indexes accessible.
    pub indexes: &'a dyn IndexOracle,
    /// Statistical estimates.
    pub stats: &'a dyn StatisticsOracle,
    /// The global ID for this query (if it exists).
    pub global_id: Option<&'a GlobalId>,
    /// Transforms can use this field to communicate information outside the result plans.
    pub dataflow_metainfo: &'a mut DataflowMetainfo,
}

impl<'a> TransformCtx<'a> {
    /// Generates a `TransformArgs` instance for the given `IndexOracle` with no `GlobalId`
    pub fn dummy(dataflow_metainfo: &'a mut DataflowMetainfo) -> Self {
        Self {
            indexes: &EmptyIndexOracle,
            stats: &EmptyStatisticsOracle,
            global_id: None,
            dataflow_metainfo,
        }
    }

    /// Generates a `TransformArgs` instance for the given `IndexOracle` and `StatisticsOracle` with
    /// a `GlobalId`
    pub fn with_id_and_stats_and_metainfo(
        indexes: &'a dyn IndexOracle,
        stats: &'a dyn StatisticsOracle,
        global_id: &'a GlobalId,
        dataflow_metainfo: &'a mut DataflowMetainfo,
    ) -> Self {
        Self {
            indexes,
            stats,
            global_id: Some(global_id),
            dataflow_metainfo,
        }
    }
}

/// Types capable of transforming relation expressions.
pub trait Transform: std::fmt::Debug {
    /// Transform a relation into a functionally equivalent relation.
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), TransformError>;

    /// A string describing the transform.
    ///
    /// This is useful mainly when iterating through many `Box<Transform>`
    /// and one wants to judge progress before some defect occurs.
    fn debug(&self) -> String {
        format!("{:?}", self)
    }
}

/// Errors that can occur during a transformation.
#[derive(Debug, Clone)]
pub enum TransformError {
    /// An unstructured error.
    Internal(String),
    /// A reference to an apparently unbound identifier.
    IdentifierMissing(mz_expr::LocalId),
    /// Notify the caller to panic with the given message.
    ///
    /// This is used to bypass catch_unwind-wrapped calls of the optimizer and
    /// support `SELECT mz_unsafe.mz_panic(<literal>)` statements as a mechanism to kill
    /// environmentd in various tests.
    CallerShouldPanic(String),
}

impl fmt::Display for TransformError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TransformError::Internal(msg) => write!(f, "internal transform error: {}", msg),
            TransformError::IdentifierMissing(i) => {
                write!(f, "apparently unbound identifier: {:?}", i)
            }
            TransformError::CallerShouldPanic(msg) => {
                write!(f, "caller should panic with message: {}", msg)
            }
        }
    }
}

impl Error for TransformError {}

impl From<RecursionLimitError> for TransformError {
    fn from(error: RecursionLimitError) -> Self {
        TransformError::Internal(error.to_string())
    }
}

/// A trait for a type that can answer questions about what indexes exist.
pub trait IndexOracle: fmt::Debug {
    /// Returns an iterator over the indexes that exist on the identified
    /// collection.
    ///
    /// Each index is described by the list of key expressions. If no indexes
    /// exist for the identified collection, or if the identified collection
    /// is unknown, the returned iterator will be empty.
    ///
    // NOTE(benesch): The allocation here is unfortunate, but on the other hand
    // you need only allocate when you actually look for an index. Can we do
    // better somehow? Making the entire optimizer generic over this iterator
    // type doesn't presently seem worthwhile.
    fn indexes_on(
        &self,
        id: GlobalId,
    ) -> Box<dyn Iterator<Item = (GlobalId, &[MirScalarExpr])> + '_>;
}

/// An [`IndexOracle`] that knows about no indexes.
#[derive(Debug)]
pub struct EmptyIndexOracle;

impl IndexOracle for EmptyIndexOracle {
    fn indexes_on(
        &self,
        _id: GlobalId,
    ) -> Box<dyn Iterator<Item = (GlobalId, &[MirScalarExpr])> + '_> {
        Box::new(iter::empty())
    }
}

/// A trait for a type that can estimate statistics about a given `GlobalId`
pub trait StatisticsOracle: fmt::Debug + Send {
    /// Returns a cardinality estimate for the given identifier
    ///
    /// Returning `None` means "no estimate"; returning `Some(0)` means estimating that the shard backing `id` is empty
    fn cardinality_estimate(&self, id: GlobalId) -> Option<usize>;
}

/// A [`StatisticsOracle`] that knows nothing and can give no estimates.
#[derive(Debug)]
pub struct EmptyStatisticsOracle;

impl StatisticsOracle for EmptyStatisticsOracle {
    fn cardinality_estimate(&self, _: GlobalId) -> Option<usize> {
        None
    }
}

/// A sequence of transformations iterated some number of times.
#[derive(Debug)]
pub struct Fixpoint {
    name: &'static str,
    transforms: Vec<Box<dyn crate::Transform>>,
    limit: usize,
}

impl Transform for Fixpoint {
    #[tracing::instrument(
        target = "optimizer",
        level = "debug",
        skip_all,
        fields(path.segment = self.name)
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        // The number of iterations for a relation to settle depends on the
        // number of nodes in the relation. Instead of picking an arbitrary
        // hard limit on the number of iterations, we use a soft limit and
        // check whether the relation has become simpler after reaching it.
        // If so, we perform another pass of transforms. Otherwise, there is
        // a bug somewhere that prevents the relation from settling on a
        // stable shape.
        loop {
            let mut original_count = 0;
            relation.try_visit_post::<_, TransformError>(&mut |_| Ok(original_count += 1))?;
            for i in 0..self.limit {
                let original = relation.clone();

                let span = tracing::span!(
                    target: "optimizer",
                    tracing::Level::DEBUG,
                    "iteration",
                    path.segment = format!("{:04}", i)
                );
                span.in_scope(|| -> Result<(), TransformError> {
                    for transform in self.transforms.iter() {
                        transform.transform(relation, ctx)?;
                    }
                    mz_repr::explain::trace_plan(relation);
                    Ok(())
                })?;

                if *relation == original {
                    mz_repr::explain::trace_plan(relation);
                    return Ok(());
                }
            }
            let mut final_count = 0;
            relation.try_visit_post::<_, TransformError>(&mut |_| Ok(final_count += 1))?;
            if final_count >= original_count {
                break;
            }
        }
        for transform in self.transforms.iter() {
            transform.transform(relation, ctx)?;
        }
        Err(TransformError::Internal(format!(
            "fixpoint looped too many times {:#?}; transformed relation: {}",
            self,
            relation.pretty()
        )))
    }
}

/// A sequence of transformations that simplify the `MirRelationExpr`
#[derive(Debug)]
pub struct FuseAndCollapse {
    transforms: Vec<Box<dyn crate::Transform>>,
}

impl Default for FuseAndCollapse {
    fn default() -> Self {
        Self {
            // TODO: The relative orders of the transforms have not been
            // determined except where there are comments.
            // TODO (#6542): All the transforms here except for `ProjectionLifting`
            //  and `RedundantJoin` can be implemented as free functions.
            transforms: vec![
                Box::new(crate::canonicalization::ProjectionExtraction),
                Box::new(crate::movement::ProjectionLifting::default()),
                Box::new(crate::fusion::Fusion),
                Box::new(crate::canonicalization::FlatMapToMap),
                Box::new(crate::fusion::join::Join),
                Box::new(crate::normalize_lets::NormalizeLets::new(false)),
                Box::new(crate::fusion::reduce::Reduce),
                Box::new(crate::compound::UnionNegateFusion),
                // This goes after union fusion so we can cancel out
                // more branches at a time.
                Box::new(crate::union_cancel::UnionBranchCancellation),
                // This should run before redundant join to ensure that key info
                // is correct.
                Box::new(crate::normalize_lets::NormalizeLets::new(false)),
                // Removes redundant inputs from joins.
                // Note that this eliminates one redundant input per join,
                // so it is necessary to run this section in a loop.
                Box::new(crate::redundant_join::RedundantJoin::default()),
                // As a final logical action, convert any constant expression to a constant.
                // Some optimizations fight against this, and we want to be sure to end as a
                // `MirRelationExpr::Constant` if that is the case, so that subsequent use can
                // clearly see this.
                Box::new(crate::fold_constants::FoldConstants { limit: Some(10000) }),
            ],
        }
    }
}

impl Transform for FuseAndCollapse {
    #[tracing::instrument(
        target = "optimizer",
        level = "debug",
        skip_all,
        fields(path.segment = "fuse_and_collapse")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        for transform in self.transforms.iter() {
            transform.transform(relation, ctx)?;
        }
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

/// Construct a normalizing transform that runs transforms that normalize the
/// structure of the tree until a fixpoint.
///
/// Care needs to be taken to ensure that the fixpoint converges for every
/// possible input tree. If this is not the case, there are two possibilities:
/// 1. The rewrite loop runs enters an oscillating cycle.
/// 2. The expression grows without bound.
pub fn normalize() -> crate::Fixpoint {
    crate::Fixpoint {
        name: "normalize",
        limit: 100,
        transforms: vec![
            Box::new(crate::normalize_lets::NormalizeLets::new(false)),
            Box::new(crate::normalize_ops::NormalizeOps),
        ],
    }
}

/// A naive optimizer for relation expressions.
///
/// The optimizer currently applies only peep-hole optimizations, from a limited
/// set that were sufficient to get some of TPC-H up and working. It is worth a
/// review at some point to improve the quality, coverage, and architecture of
/// the optimizations.
#[derive(Debug)]
pub struct Optimizer {
    /// A logical name identifying this optimizer instance.
    pub name: &'static str,
    /// The list of transforms to apply to an input relation.
    pub transforms: Vec<Box<dyn crate::Transform>>,
}

impl Optimizer {
    /// Builds a logical optimizer that only performs logical transformations.
    #[deprecated = "Create an Optimize instance and call `optimize` instead."]
    pub fn logical_optimizer(ctx: &crate::typecheck::SharedContext) -> Self {
        let transforms: Vec<Box<dyn crate::Transform>> = vec![
            Box::new(crate::typecheck::Typecheck::new(Arc::clone(ctx)).strict_join_equivalences()),
            // 1. Structure-agnostic cleanup
            Box::new(normalize()),
            Box::new(crate::non_null_requirements::NonNullRequirements::default()),
            // 2. Collapse constants, joins, unions, and lets as much as possible.
            // TODO: lift filters/maps to maximize ability to collapse
            // things down?
            Box::new(crate::Fixpoint {
                name: "fixpoint",
                limit: 100,
                transforms: vec![Box::new(crate::FuseAndCollapse::default())],
            }),
            // 3. Structure-aware cleanup that needs to happen before ColumnKnowledge
            Box::new(crate::threshold_elision::ThresholdElision),
            // 4. Move predicate information up and down the tree.
            //    This also fixes the shape of joins in the plan.
            Box::new(crate::Fixpoint {
                name: "fixpoint",
                limit: 100,
                transforms: vec![
                    // Predicate pushdown sets the equivalence classes of joins.
                    Box::new(crate::predicate_pushdown::PredicatePushdown::default()),
                    // Lifts the information `!isnull(col)`
                    Box::new(crate::nonnullable::NonNullable),
                    // Lifts the information `col = literal`
                    // TODO (#6613): this also tries to lift `!isnull(col)` but
                    // less well than the previous transform. Eliminate
                    // redundancy between the two transforms.
                    Box::new(crate::column_knowledge::ColumnKnowledge::default()),
                    // Lifts the information `col1 = col2`
                    Box::new(crate::demand::Demand::default()),
                    Box::new(crate::FuseAndCollapse::default()),
                ],
            }),
            // 5. Reduce/Join simplifications.
            Box::new(crate::Fixpoint {
                name: "fixpoint",
                limit: 100,
                transforms: vec![
                    Box::new(crate::semijoin_idempotence::SemijoinIdempotence::default()),
                    // Pushes aggregations down
                    Box::new(crate::reduction_pushdown::ReductionPushdown),
                    // Replaces reduces with maps when the group keys are
                    // unique with maps
                    Box::new(crate::reduce_elision::ReduceElision),
                    // Converts `Cross Join {Constant(Literal) + Input}` to
                    // `Map {Cross Join (Input, Constant()), Literal}`.
                    // Join fusion will clean this up to `Map{Input, Literal}`
                    Box::new(crate::literal_lifting::LiteralLifting::default()),
                    // Identifies common relation subexpressions.
                    Box::new(crate::cse::relation_cse::RelationCSE::new(false)),
                    Box::new(crate::FuseAndCollapse::default()),
                ],
            }),
            Box::new(
                crate::typecheck::Typecheck::new(Arc::clone(ctx))
                    .disallow_new_globals()
                    .strict_join_equivalences(),
            ),
        ];
        Self {
            name: "logical",
            transforms,
        }
    }

    /// Builds a physical optimizer.
    ///
    /// Performs logical transformations followed by all physical ones.
    /// This is meant to be used for optimizing each view within a dataflow
    /// once view inlining has already happened, right before dataflow
    /// rendering.
    pub fn physical_optimizer(ctx: &crate::typecheck::SharedContext) -> Self {
        // Implementation transformations
        let transforms: Vec<Box<dyn crate::Transform>> = vec![
            Box::new(
                crate::typecheck::Typecheck::new(Arc::clone(ctx))
                    .disallow_new_globals()
                    .strict_join_equivalences(),
            ),
            // Considerations for the relationship between JoinImplementation and other transforms:
            // - there should be a run of LiteralConstraints before JoinImplementation lifts away
            //   the Filters from the Gets;
            // - there should be no RelationCSE between this LiteralConstraints and
            //   JoinImplementation, because that could move an IndexedFilter behind a Get.
            // - The last RelationCSE before JoinImplementation should be with inline_mfp = true.
            // - Currently, JoinImplementation can't be before LiteralLifting because the latter
            //   sometimes creates `Unimplemented` joins (despite LiteralLifting already having been
            //   run in the logical optimizer).
            // - Not running ColumnKnowledge in the same fixpoint loop with JoinImplementation
            //   is slightly hurting our plans. However, I'd say we should fix these problems by
            //   making ColumnKnowledge (and/or JoinImplementation) smarter (#18051), rather than
            //   having them in the same fixpoint loop. If they would be in the same fixpoint loop,
            //   then we either run the risk of ColumnKnowledge invalidating a join plan (#17993),
            //   or we would have to run JoinImplementation an unbounded number of times, which is
            //   also not good #16076.
            //   (The same is true for FoldConstants, Demand, and LiteralLifting to a lesser
            //   extent.)
            //
            // Also note that FoldConstants and LiteralLifting are not confluent. They can
            // oscillate between e.g.:
            //         Constant
            //           - (4)
            // and
            //         Map (4)
            //           Constant
            //             - ()
            Box::new(crate::Fixpoint {
                name: "fixpoint",
                limit: 100,
                transforms: vec![
                    Box::new(crate::column_knowledge::ColumnKnowledge::default()),
                    Box::new(crate::fold_constants::FoldConstants { limit: Some(10000) }),
                    Box::new(crate::demand::Demand::default()),
                    Box::new(crate::literal_lifting::LiteralLifting::default()),
                ],
            }),
            Box::new(crate::literal_constraints::LiteralConstraints),
            Box::new(crate::Fixpoint {
                name: "fix_joins",
                limit: 100,
                transforms: vec![Box::new(
                    crate::join_implementation::JoinImplementation::default(),
                )],
            }),
            Box::new(crate::canonicalize_mfp::CanonicalizeMfp),
            // Identifies common relation subexpressions.
            Box::new(crate::cse::relation_cse::RelationCSE::new(false)),
            Box::new(crate::fold_constants::FoldConstants { limit: Some(10000) }),
            // Remove threshold operators which have no effect.
            // Must be done at the very end of the physical pass, because before
            // that (at least at the moment) we cannot be sure that all trees
            // are simplified equally well so they are structurally almost
            // identical. Check the `threshold_elision.slt` tests that fail if
            // you remove this transform for examples.
            Box::new(crate::threshold_elision::ThresholdElision),
            // We need this to ensure that `CollectIndexRequests` gets a normalized plan.
            // (For example, `FoldConstants` can break the normalized form by removing all
            // references to a Let, see https://github.com/MaterializeInc/materialize/issues/21175)
            Box::new(crate::normalize_lets::NormalizeLets::new(false)),
            Box::new(crate::typecheck::Typecheck::new(Arc::clone(ctx)).disallow_new_globals()),
        ];
        Self {
            name: "physical",
            transforms,
        }
    }

    /// Contains the logical optimizations that should run after cross-view
    /// transformations run.
    ///
    /// Set `allow_new_globals` when you will use these as the first passes.
    /// The first instance of the typechecker in an optimizer pipeline should
    /// allow new globals (or it will crash when it encounters them).
    pub fn logical_cleanup_pass(
        ctx: &crate::typecheck::SharedContext,
        allow_new_globals: bool,
    ) -> Self {
        let mut typechecker =
            crate::typecheck::Typecheck::new(Arc::clone(ctx)).strict_join_equivalences();

        if !allow_new_globals {
            typechecker = typechecker.disallow_new_globals();
        }

        let transforms: Vec<Box<dyn crate::Transform>> = vec![
            Box::new(typechecker),
            // Delete unnecessary maps.
            Box::new(crate::fusion::Fusion),
            Box::new(crate::Fixpoint {
                name: "fixpoint",
                limit: 100,
                transforms: vec![
                    Box::new(crate::canonicalize_mfp::CanonicalizeMfp),
                    // Remove threshold operators which have no effect.
                    Box::new(crate::threshold_elision::ThresholdElision),
                    // Projection pushdown may unblock fusing joins and unions.
                    Box::new(crate::fusion::join::Join),
                    Box::new(crate::redundant_join::RedundantJoin::default()),
                    // Redundant join produces projects that need to be fused.
                    Box::new(crate::fusion::Fusion),
                    Box::new(crate::compound::UnionNegateFusion),
                    // This goes after union fusion so we can cancel out
                    // more branches at a time.
                    Box::new(crate::union_cancel::UnionBranchCancellation),
                    // The last RelationCSE before JoinImplementation should be with
                    // inline_mfp = true.
                    Box::new(crate::cse::relation_cse::RelationCSE::new(true)),
                    Box::new(crate::fold_constants::FoldConstants { limit: Some(10000) }),
                ],
            }),
            Box::new(
                crate::typecheck::Typecheck::new(Arc::clone(ctx))
                    .disallow_new_globals()
                    .strict_join_equivalences(),
            ),
        ];
        Self {
            name: "logical_cleanup",
            transforms,
        }
    }

    /// Optimizes the supplied relation expression.
    ///
    /// These optimizations are performed with no information about available arrangements,
    /// which makes them suitable for pre-optimization before dataflow deployment.
    #[tracing::instrument(
        target = "optimizer",
        level = "debug",
        skip_all,
        fields(path.segment = self.name)
    )]
    pub fn optimize(
        &self,
        mut relation: MirRelationExpr,
    ) -> Result<mz_expr::OptimizedMirRelationExpr, TransformError> {
        let mut dataflow_metainfo = DataflowMetainfo::default();
        let mut transform_ctx = TransformCtx::dummy(&mut dataflow_metainfo);
        let transform_result = self.transform(&mut relation, &mut transform_ctx);

        // Make sure we are not swallowing any notice.
        // TODO: we should actually wire up notices that come from here. This is not urgent, because
        // currently notices can only come from the physical MIR optimizer (specifically,
        // `LiteralConstraints`), and callers of this method are running the logical MIR optimizer.
        soft_assert_or_log!(
            dataflow_metainfo.optimizer_notices.is_empty(),
            "logical MIR optimization unexpectedly produced notices"
        );

        match transform_result {
            Ok(_) => {
                mz_repr::explain::trace_plan(&relation);
                Ok(mz_expr::OptimizedMirRelationExpr(relation))
            }
            Err(e) => {
                // Without this, the dropping of `relation` (which happens automatically when
                // returning from this function) might run into a stack overflow, see
                // https://github.com/MaterializeInc/materialize/issues/14141
                relation.destroy_carefully();
                error!("Optimizer::optimize(): {}", e);
                Err(e)
            }
        }
    }

    /// Optimizes the supplied relation expression in place, using available arrangements.
    ///
    /// This method should only be called with non-empty `indexes` when optimizing a dataflow,
    /// as the optimizations may lock in the use of arrangements that may cease to exist.
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        args: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        for transform in self.transforms.iter() {
            transform.transform(relation, args)?;
        }

        Ok(())
    }
}
