//! Dynamic congestion-based concurrency limits for controlling backpressure.

pub mod aggregators;
mod limiter;
pub mod limits;
mod mov_avgs;

pub use limiter::{Limiter, LimiterState, Outcome, Token};
