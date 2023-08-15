use async_trait::async_trait;

use super::{LimitAlgorithm, Sample};

/// A simple, fixed concurrency limit.
pub struct Fixed(u32);
impl Fixed {
    pub fn new(limit: u32) -> Self {
        assert!(limit > 0);

        Self(limit)
    }
}

#[async_trait]
impl LimitAlgorithm for Fixed {
    fn limit(&self) -> u32 {
        self.0
    }
    async fn update(&self, _reading: Sample) -> u32 {
        self.0
    }
}
