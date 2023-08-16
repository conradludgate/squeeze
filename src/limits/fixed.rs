use async_trait::async_trait;

use super::{LimitAlgorithm, Sample};

/// A simple, fixed concurrency limit.
#[derive(Clone, Copy)]
pub struct Fixed;

#[async_trait]
impl LimitAlgorithm for Fixed {
    async fn update(&mut self, old_limit: usize, _reading: Sample) -> usize {
        old_limit
    }
}
