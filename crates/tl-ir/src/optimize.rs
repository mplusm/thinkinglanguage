// ThinkingLanguage — IR Optimizer Pipeline
// Applies all optimization passes in sequence.

use crate::passes::column_pruning::prune_columns;
use crate::passes::cse::eliminate_common_subexprs;
use crate::passes::filter_merge::merge_filters;
use crate::passes::predicate_pushdown::push_predicates_down;
use crate::plan::QueryPlan;

/// Apply all optimization passes to a query plan.
pub fn optimize(plan: QueryPlan) -> QueryPlan {
    let plan = merge_filters(plan);
    let plan = push_predicates_down(plan);
    let plan = prune_columns(plan);
    let plan = eliminate_common_subexprs(plan);
    plan
}
