use std::time::Duration;

use async_trait::async_trait;

use crate::Outcome;

use super::{defaults::MIN_SAMPLE_LATENCY, LimitAlgorithm, Sample};

/// Loss- and delay-based congestion avoidance.
///
/// Additive increase, additive decrease.
///
/// Estimates queuing delay by comparing the current latency with the minimum observed latency to
/// estimate the number of jobs being queued.
///
/// For greater stability consider wrapping with a percentile window sampler. This calculates
/// a percentile (e.g. P90) over a period of time and provides that as a sample. Vegas then compares
/// recent P90 latency with the minimum observed P90. Used this way, Vegas can handle heterogeneous
/// workloads, as long as the percentile latency is fairly stable.
///
/// Can fairly distribute concurrency between independent clients as long as there is enough server
/// capacity to handle the requests. That is: as long as the server isn't overloaded and failing to
/// handle requests as a result.
///
/// Inspired by TCP Vegas.
///
/// - [TCP Vegas: End to End Congestion Avoidance on a Global
///   Internet](https://www.cs.princeton.edu/courses/archive/fall06/cos561/papers/vegas.pdf)
/// - [Understanding TCP Vegas: Theory and
/// Practice](https://www.cs.princeton.edu/research/techreps/TR-628-00)
#[derive(Clone)]
pub struct Vegas {
    min_limit: usize,
    max_limit: usize,

    // /// Lower queueing threshold, as a function of the current limit.
    // alpha: Box<dyn (Fn(u32) -> u32) + Send + Sync>,
    // /// Upper queueing threshold, as a function of the current limit.
    // beta: Box<dyn (Fn(u32) -> u32) + Send + Sync>,
    min_latency: Duration,
}

impl Vegas {
    const DEFAULT_MIN_LIMIT: usize = 1;
    const DEFAULT_MAX_LIMIT: usize = 1000;

    const DEFAULT_INCREASE_MIN_UTILISATION: f64 = 0.8;

    pub fn new() -> Self {
        Self {
            min_limit: Self::DEFAULT_MIN_LIMIT,
            max_limit: Self::DEFAULT_MAX_LIMIT,

            // alpha: Box::new(|limit| 3 * limit.ilog10().max(1)),
            // beta: Box::new(|limit| 6 * limit.ilog10().max(1)),
            min_latency: Duration::MAX,
        }
    }

    fn alpha(limit: usize) -> u32 {
        3 * limit.ilog10().max(1)
    }

    fn beta(limit: usize) -> u32 {
        6 * limit.ilog10().max(1)
    }

    pub fn with_max_limit(self, max: usize) -> Self {
        assert!(max > 0);
        Self {
            max_limit: max,
            ..self
        }
    }
}

impl Default for Vegas {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LimitAlgorithm for Vegas {
    /// Vegas algorithm, generally applied once every RTT:
    ///
    /// ```text
    /// MIN_D = estimated min. latency with no queueing
    /// D(t)  = observed latency for a job at time t
    /// L(t)  = concurrency limit at time t
    /// F(t)  = jobs in flight at time t
    ///
    /// L(t) / MIN_D = E = expected rate (no queueing)
    /// L(t) / D(t)  = A = actual rate
    ///
    /// E - A = DIFF [>= 0]
    ///
    /// alpha = low rate threshold: too little queueing
    /// beta  = high rate threshold: too much queueing
    ///
    /// L(t+1) = L(t) + 1 if DIFF < alpha and F(t) > L(t) / 2
    ///               - 1 if DIFF > beta
    /// ```
    ///
    /// Or, using queue size instead of rate:
    ///
    /// ```text
    /// queue_size = L(t) * (1 − MIN_D / D(T)) [>= 0]
    ///
    /// alpha = low queueing threshold
    /// beta  = high queueing threshold
    ///
    /// L(t+1) = L(t) + 1 if queue_size < alpha and F(t) > L(t) / 2
    ///               - 1 if queue_size > beta
    /// ```
    ///
    /// Example estimated queue sizes when `L(t)` = 10 and `MIN_D` = 10ms, for several changes in
    /// latency:
    ///
    /// ```text
    ///  10x => queue_size = 10 * (1 - 0.01 / 0.1)   =   9 (90%)
    ///   2x => queue_size = 10 * (1 - 0.01 / 0.02)  =   5 (50%)
    /// 1.5x => queue_size = 10 * (1 - 0.01 / 0.015) =   3 (30%)
    ///   1x => queue_size = 10 * (1 - 0.01 / 0.01)  =   0 (0%)
    /// 0.5x => queue_size = 10 * (1 - 0.01 / 0.005) = -10 (0%)
    /// ```
    async fn update(&mut self, old_limit: usize, sample: Sample) -> usize {
        if sample.latency < MIN_SAMPLE_LATENCY {
            return old_limit;
        }

        if sample.latency < self.min_latency {
            self.min_latency = sample.latency;
            return old_limit;
        }

        // TODO: periodically reset min. latency measurement.

        let dt = sample.latency.as_secs_f64();
        let min_d = self.min_latency.as_secs_f64();

        let estimated_queued_jobs = (old_limit as f64 * (1.0 - (min_d / dt))).ceil() as u32;

        let utilisation = sample.in_flight as f64 / old_limit as f64;

        let increment = old_limit.ilog10().max(1) as usize;

        let limit =
            // Limit too big
            if sample.outcome == Outcome::Overload || estimated_queued_jobs < Self::alpha(old_limit) {
                old_limit - increment

            // Limit too small
            } else if estimated_queued_jobs > Self::beta(old_limit)
                && utilisation > Self::DEFAULT_INCREASE_MIN_UTILISATION
            {
                // TODO: support some kind of fast start, e.g. increase by beta when almost no queueing
                old_limit + increment

            // Perfect porridge
            } else {
                old_limit
            };

        limit.clamp(self.min_limit, self.max_limit)
    }
}
