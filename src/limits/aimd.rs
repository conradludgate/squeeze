use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;

use crate::{limits::Sample, Outcome};

use super::LimitAlgorithm;

/// Loss-based congestion avoidance.
///
/// Additive-increase, multiplicative decrease.
///
/// Adds available currency when:
/// 1. no load-based errors are observed, and
/// 2. the utilisation of the current limit is high.
///
/// Reduces available concurrency by a factor when load-based errors are detected.
pub struct Aimd {
    min_limit: u32,
    max_limit: u32,
    decrease_factor: f32,
    increase_by: u32,
    min_utilisation_threshold: f64,

    limit: AtomicU32,
}

impl Aimd {
    const DEFAULT_DECREASE_FACTOR: f32 = 0.9;
    const DEFAULT_INCREASE: u32 = 1;
    const DEFAULT_MIN_LIMIT: u32 = 1;
    const DEFAULT_MAX_LIMIT: u32 = 1000;
    const DEFAULT_INCREASE_MIN_UTILISATION: f64 = 0.8;

    pub fn with_initial_limit(initial_limit: u32) -> Self {
        assert!(initial_limit > 0);

        Self {
            min_limit: Self::DEFAULT_MIN_LIMIT,
            max_limit: Self::DEFAULT_MAX_LIMIT,
            decrease_factor: Self::DEFAULT_DECREASE_FACTOR,
            increase_by: Self::DEFAULT_INCREASE,
            min_utilisation_threshold: Self::DEFAULT_INCREASE_MIN_UTILISATION,

            limit: AtomicU32::new(initial_limit),
        }
    }

    pub fn decrease_factor(self, factor: f32) -> Self {
        assert!((0.5..1.0).contains(&factor));
        Self {
            decrease_factor: factor,
            ..self
        }
    }

    pub fn increase_by(self, increase: u32) -> Self {
        assert!(increase > 0);
        Self {
            increase_by: increase,
            ..self
        }
    }

    pub fn with_max_limit(self, max: u32) -> Self {
        assert!(max > 0);
        Self {
            max_limit: max,
            ..self
        }
    }

    /// A threshold below which the limit won't be increased. 0.5 = 50%.
    pub fn with_min_utilisation_threshold(self, min_util: f64) -> Self {
        assert!(min_util > 0. && min_util < 1.);
        Self {
            min_utilisation_threshold: min_util,
            ..self
        }
    }
}

#[async_trait]
impl LimitAlgorithm for Aimd {
    fn limit(&self) -> u32 {
        self.limit.load(Ordering::Acquire)
    }

    async fn update(&self, sample: Sample) -> u32 {
        use Outcome::*;
        match sample.outcome {
            Success => {
                self.limit
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |limit| {
                        let utilisation = sample.in_flight as f64 / limit as f64;

                        if utilisation > self.min_utilisation_threshold {
                            let limit = limit + self.increase_by;
                            Some(limit.clamp(self.min_limit, self.max_limit))
                        } else {
                            Some(limit)
                        }
                    })
                    .expect("we always return Some(limit)");
            }
            Overload => {
                self.limit
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |limit| {
                        let limit = limit as f32 * self.decrease_factor;

                        // Floor instead of round, so the limit reduces even with small numbers.
                        // E.g. round(2 * 0.9) = 2, but floor(2 * 0.9) = 1
                        let limit = limit.floor() as u32;

                        Some(limit.clamp(self.min_limit, self.max_limit))
                    })
                    .expect("we always return Some(limit)");
            }
        }
        self.limit.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Notify;

    use crate::Limiter;

    use super::*;

    #[tokio::test]
    async fn should_decrease_limit_on_overload() {
        let aimd = Aimd::with_initial_limit(10)
            .decrease_factor(0.5)
            .increase_by(1);

        let release_notifier = Arc::new(Notify::new());

        let limiter = Limiter::new(aimd).with_release_notifier(release_notifier.clone());

        let token = limiter.try_acquire().unwrap();
        limiter.release(token, Some(Outcome::Overload)).await;
        release_notifier.notified().await;
        assert_eq!(limiter.state().limit(), 5, "overload: decrease");
    }

    #[tokio::test]
    async fn should_increase_limit_on_success_when_using_gt_util_threshold() {
        let aimd = Aimd::with_initial_limit(4)
            .decrease_factor(0.5)
            .increase_by(1)
            .with_min_utilisation_threshold(0.5);

        let limiter = Limiter::new(aimd);

        let token = limiter.try_acquire().unwrap();
        let _token = limiter.try_acquire().unwrap();
        let _token = limiter.try_acquire().unwrap();

        limiter.release(token, Some(Outcome::Success)).await;
        assert_eq!(limiter.state().limit(), 5, "success: increase");
    }

    #[tokio::test]
    async fn should_not_change_limit_on_success_when_using_lt_util_threshold() {
        let aimd = Aimd::with_initial_limit(4)
            .decrease_factor(0.5)
            .increase_by(1)
            .with_min_utilisation_threshold(0.5);

        let limiter = Limiter::new(aimd);

        let token = limiter.try_acquire().unwrap();

        limiter.release(token, Some(Outcome::Success)).await;
        assert_eq!(
            limiter.state().limit(),
            4,
            "success: ignore when < half limit"
        );
    }

    #[tokio::test]
    async fn should_not_change_limit_when_no_outcome() {
        let aimd = Aimd::with_initial_limit(10)
            .decrease_factor(0.5)
            .increase_by(1);

        let limiter = Limiter::new(aimd);

        let token = limiter.try_acquire().unwrap();
        limiter.release(token, None).await;
        assert_eq!(limiter.state().limit(), 10, "ignore");
    }
}
