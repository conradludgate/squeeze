use async_trait::async_trait;

use crate::{
    limits::Sample,
    mov_avgs::{ExpSmoothed, Simple},
};

use super::{defaults::MIN_SAMPLE_LATENCY, LimitAlgorithm};

/// Delay-based congestion avoidance.
///
/// Additive-increase, multiplicative decrease based on change in average latency.
///
/// Considers the difference in average latency between a short time window and a longer window.
/// Changes in these values is considered an indicator of a change in load on the system.
///
/// Inspired by TCP congestion control algorithms using delay gradients.
///
/// - [Revisiting TCP Congestion Control Using Delay Gradients](https://hal.science/hal-01597987/)
pub struct Gradient {
    min_limit: u32,
    max_limit: u32,

    long_window_latency: ExpSmoothed,
    short_window_latency: Simple,
    limit: f64,
}

impl Gradient {
    const DEFAULT_MIN_LIMIT: u32 = 1;
    const DEFAULT_MAX_LIMIT: u32 = 1000;

    const DEFAULT_INCREASE: f64 = 4.;
    const DEFAULT_INCREASE_MIN_UTILISATION: f64 = 0.8;
    const DEFAULT_INCREASE_MIN_GRADIENT: f64 = 0.9;

    const DEFAULT_LONG_WINDOW_SAMPLES: u16 = 500;
    const DEFAULT_SHORT_WINDOW_SAMPLES: u16 = 10;

    const DEFAULT_TOLERANCE: f64 = 2.;
    const DEFAULT_SMOOTHING: f64 = 0.2;

    pub fn new() -> Self {
        Self {
            min_limit: Self::DEFAULT_MIN_LIMIT,
            max_limit: Self::DEFAULT_MAX_LIMIT,

            long_window_latency: ExpSmoothed::window_size(Self::DEFAULT_LONG_WINDOW_SAMPLES),
            short_window_latency: Simple::window_size(Self::DEFAULT_SHORT_WINDOW_SAMPLES),
            limit: 0.0,
        }
    }

    pub fn with_max_limit(self, max: u32) -> Self {
        assert!(max > 0);
        Self {
            max_limit: max,
            ..self
        }
    }
}

impl Default for Gradient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LimitAlgorithm for Gradient {
    async fn update(&mut self, old_limit: usize, sample: Sample) -> usize {
        // FIXME: Improve or justify safety of numerical conversions
        if sample.latency < MIN_SAMPLE_LATENCY {
            return old_limit;
        }

        // Update short window
        let short = self.short_window_latency.sample(sample.latency);

        // Update long window
        let long = self.long_window_latency.sample(sample.latency);

        let long_short_ratio = long.as_secs_f64() / short.as_secs_f64();

        // Speed up return to baseline after long period of increased load.
        if long_short_ratio > 2.0 {
            self.long_window_latency.set(long.mul_f64(0.95));
        }

        let old_limit = if self.limit == 0.0 {
            old_limit as f64
        } else {
            self.limit
        };

        // Only apply downwards gradient (when latency has increased).
        // Limit to >= 0.5 to prevent aggressive load shedding.
        // Tolerate a given amount of latency difference.
        let gradient = (Self::DEFAULT_TOLERANCE * long_short_ratio).clamp(0.5, 1.0);

        let utilisation = sample.in_flight as f64 / old_limit;

        // Only apply an increase if we're using enough to justify it
        // and we're not trying to reduce the limit by much.
        let increase = if utilisation > Self::DEFAULT_INCREASE_MIN_UTILISATION
            && gradient > Self::DEFAULT_INCREASE_MIN_GRADIENT
        {
            Self::DEFAULT_INCREASE
        } else {
            0.0
        };

        // Apply gradient, and allow an additive increase.
        let mut new_limit = old_limit * gradient + increase;
        new_limit =
            old_limit * (1.0 - Self::DEFAULT_SMOOTHING) + new_limit * Self::DEFAULT_SMOOTHING;

        new_limit = (new_limit).clamp(self.min_limit as f64, self.max_limit as f64);

        self.limit = new_limit;
        new_limit as usize
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{Limiter, Outcome};

    use super::*;

    #[tokio::test]
    async fn it_works() {
        static INIT_LIMIT: usize = 10;
        let gradient = Gradient::new();

        let limiter = Limiter::new(gradient, INIT_LIMIT);

        /*
         * Concurrency = 10
         * Steady latency
         */
        let mut tokens = Vec::with_capacity(10);
        for _ in 0..10 {
            let token = limiter.try_acquire().unwrap();
            tokens.push(token);
        }
        for mut token in tokens {
            token.set_latency(Duration::from_millis(25));
            limiter.release(token, Some(Outcome::Success)).await;
        }
        let higher_limit = limiter.state().limit();
        assert!(
            higher_limit > INIT_LIMIT,
            "steady latency + high concurrency: increase limit"
        );

        /*
         * Concurrency = 10
         * 10x previous latency
         */
        let mut tokens = Vec::with_capacity(10);
        for _ in 0..10 {
            let mut token = limiter.try_acquire().unwrap();
            token.set_latency(Duration::from_millis(250));
            tokens.push(token);
        }
        for token in tokens {
            limiter.release(token, Some(Outcome::Success)).await;
        }
        assert!(
            limiter.state().limit() < higher_limit,
            "increased latency: decrease limit"
        );
    }
}
