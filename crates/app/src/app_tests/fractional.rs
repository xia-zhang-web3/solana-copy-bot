//! Scoped fractional handoff tests; reuse accepted fixtures, never old books.
use super::{
    association_fixture, association_parent_fixture, association_sell_fixture, b135_fixture,
    b93_fixture, generic_sell_synthetic_fixture,
};
#[path = "fractional_baseline_tests.rs"]
mod fractional_baseline_tests;
#[path = "fractional_boundary_tests.rs"]
mod fractional_boundary_tests;
#[path = "fractional_financial_fixture.rs"]
mod fractional_financial_fixture;
#[path = "fractional_financial_tests.rs"]
mod fractional_financial_tests;
#[path = "fractional_fixture.rs"]
mod fractional_fixture;
#[path = "fractional_synthetic_fixture.rs"]
mod fractional_synthetic_fixture;
#[path = "fractional_tests.rs"]
mod fractional_tests;
#[path = "fractional_transport_fixture.rs"]
mod fractional_transport_fixture;
#[path = "fractional_transport_tests.rs"]
mod fractional_transport_tests;
#[path = "native_buy_baseline_tests.rs"]
mod native_buy_baseline_tests;
#[path = "native_buy_route_mock_tests.rs"]
mod native_buy_route_mock_tests;
#[path = "native_buy_rpc_tests.rs"]
mod native_buy_rpc_tests;
#[path = "native_buy_runner_tests.rs"]
mod native_buy_runner_tests;
#[path = "native_buy_cohort_tests.rs"]
mod native_buy_cohort_tests;
