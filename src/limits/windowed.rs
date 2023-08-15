use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use crate::aggregators::Aggregator;

use super::{defaults::MIN_SAMPLE_LATENCY, LimitAlgorithm, Sample};

/// A wrapper around a [LimitAlgorithm] which aggregates samples within a window, periodically
/// updating the limit.
///
/// The window duration is dynamic, based on the latency of the previous aggregated sample.
///
/// Various [aggregators](crate::aggregators) are available to aggregate samples.
#[derive(Clone, Copy)]
pub struct Windowed<L, S> {
    min_window: Duration,
    max_window: Duration,
    min_samples: usize,
    min_latency: Duration,

    inner: L,

    window: Window<S>,
}

#[derive(Clone, Copy)]
struct Window<S> {
    aggregator: S,

    start: Instant,
    duration: Duration,
}

impl<L: LimitAlgorithm, S: Aggregator> Windowed<L, S> {
    pub fn new(inner: L, sampler: S) -> Self {
        let min_window = Duration::from_micros(1);
        Self {
            min_window,
            max_window: Duration::from_secs(1),
            min_samples: 10,
            min_latency: MIN_SAMPLE_LATENCY,

            inner,

            window: Window {
                aggregator: sampler,
                duration: min_window,
                start: Instant::now(),
            },
        }
    }

    /// At least this many samples need to be aggregated before updating the limit.
    pub fn with_min_samples(mut self, samples: usize) -> Self {
        assert!(samples > 0, "at least one sample required per window");
        self.min_samples = samples;
        self
    }

    /// Minimum time to wait before attempting to update the limit.
    pub fn with_min_window(mut self, min: Duration) -> Self {
        self.min_window = min;
        self
    }

    /// Maximum time to wait before attempting to update the limit.
    ///
    /// Will wait for longer if not enough samples have been aggregated. See
    /// [with_min_samples()](Self::with_min_samples()).
    pub fn with_max_window(mut self, max: Duration) -> Self {
        self.max_window = max;
        self
    }
}

#[async_trait]
impl<L, S> LimitAlgorithm for Windowed<L, S>
where
    L: LimitAlgorithm + Send + Sync + Clone,
    S: Aggregator + Send + Sync + Clone,
{
    async fn update(mut self, old_limit: u32, sample: Sample) -> (Self, u32) {
        if sample.latency < self.min_latency {
            return (self, old_limit);
        }

        let agg_sample = self.window.aggregator.sample(sample);

        if self.window.aggregator.sample_size() >= self.min_samples
            && self.window.start.elapsed() >= self.window.duration
        {
            self.window.aggregator.reset();

            self.window.start = Instant::now();

            // TODO: the Netflix lib uses 2x min latency, make this configurable?
            self.window.duration = agg_sample.latency.clamp(self.min_window, self.max_window);

            let (inner, limit) = self.inner.update(old_limit, agg_sample).await;
            self.inner = inner;
            (self, limit)
        } else {
            (self, old_limit)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{aggregators::Average, limits::Vegas, Outcome};

    use super::*;

    #[tokio::test]
    async fn it_works() {
        let samples = 2;

        // Just test with a min sample size for now
        let mut windowed_vegas = Windowed::new(Vegas::new(), Average::default())
            .with_min_samples(samples)
            .with_min_window(Duration::ZERO)
            .with_max_window(Duration::ZERO);

        let mut limit = 10;

        for _ in 0..samples {
            (windowed_vegas, limit) = windowed_vegas
                .update(
                    limit,
                    Sample {
                        in_flight: 1,
                        latency: Duration::from_millis(10),
                        outcome: Outcome::Success,
                    },
                )
                .await;
        }
        assert_eq!(limit, 10, "first window shouldn't change limit for Vegas");

        for _ in 0..samples {
            (windowed_vegas, limit) = windowed_vegas
                .update(
                    limit,
                    Sample {
                        in_flight: 1,
                        latency: Duration::from_millis(100),
                        outcome: Outcome::Overload,
                    },
                )
                .await;
        }
        assert!(limit < 10, "limit should be reduced");
    }
}
