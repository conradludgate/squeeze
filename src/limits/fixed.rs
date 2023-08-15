use async_trait::async_trait;

use super::{LimitAlgorithm, Sample};

/// A simple, fixed concurrency limit.
#[derive(Clone, Copy)]
pub struct Fixed;

#[async_trait]
impl LimitAlgorithm for Fixed {
    async fn update(self, old_limit: u32, _reading: Sample) -> (Self, u32) {
        (self, old_limit)
    }
}
