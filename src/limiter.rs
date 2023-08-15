use std::{
    cmp,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    sync::{Semaphore, TryAcquireError},
    time::{timeout, Instant},
};

use crate::limits::{LimitAlgorithm, Sample};

/// Limits the number of concurrent jobs.
///
/// Concurrency is limited through the use of [Token]s. Acquire a token to run a job, and release the
/// token once the job is finished.
///
/// The limit will be automatically adjusted based on observed latency (delay) and/or failures
/// caused by overload (loss).
#[derive(Debug)]
pub struct Limiter<T> {
    limit_algo: T,
    inner: LimiterInner,
}

#[derive(Debug)]
struct LimiterInner {
    semaphore: Arc<Semaphore>,
    limit: AtomicUsize,

    /// Best-effort
    in_flight: AtomicUsize,

    #[cfg(test)]
    notifier: Option<Arc<tokio::sync::Notify>>,
}

/// A concurrency token, required to run a job.
///
/// Release the token back to the [Limiter] after the job is complete.
#[derive(Debug)]
pub struct Token<'t> {
    limiter: &'t LimiterInner,
    start: Instant,
}

/// A snapshot of the state of the [Limiter].
///
/// Not guaranteed to be consistent under high concurrency.
#[derive(Debug, Clone, Copy)]
pub struct LimiterState {
    limit: usize,
    available: usize,
    in_flight: usize,
}

/// Whether a job succeeded or failed as a result of congestion/overload.
///
/// Errors not considered to be caused by overload should be ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The job succeeded, or failed in a way unrelated to overload.
    Success,
    /// The job failed because of overload, e.g. it timed out or an explicit backpressure signal
    /// was observed.
    Overload,
}

impl<T> Limiter<T>
where
    T: LimitAlgorithm,
{
    /// Create a limiter with a given limit control algorithm.
    pub fn new(limit_algo: T) -> Self {
        let initial_permits = limit_algo.limit();
        assert!(initial_permits > 0);
        Self {
            limit_algo,
            inner: LimiterInner {
                semaphore: Arc::new(Semaphore::new(initial_permits)),
                limit: AtomicUsize::new(initial_permits),
                in_flight: AtomicUsize::new(0),

                #[cfg(test)]
                notifier: None,
            },
        }
    }

    /// In some cases [Token]s are acquired asynchronously when updating the limit.
    #[cfg(test)]
    pub fn with_release_notifier(mut self, n: Arc<tokio::sync::Notify>) -> Self {
        self.inner.notifier = Some(n);
        self
    }

    /// Try to immediately acquire a concurrency [Token].
    ///
    /// Returns `None` if there are none available.
    pub fn try_acquire(&self) -> Option<Token<'_>> {
        match self.inner.semaphore.try_acquire() {
            Ok(permit) => {
                permit.forget();
                self.inner.in_flight.fetch_add(1, Ordering::AcqRel);
                Some(Token::new(&self.inner))
            }
            Err(TryAcquireError::NoPermits) => None,
            Err(TryAcquireError::Closed) => {
                panic!("we own the semaphore, we shouldn't have closed it")
            }
        }
    }

    /// Try to acquire a concurrency [Token], waiting for `duration` if there are none available.
    ///
    /// Returns `None` if there are none available after `duration`.
    pub async fn acquire_timeout(&self, duration: Duration) -> Option<Token<'_>> {
        match timeout(duration, self.inner.semaphore.acquire()).await {
            Ok(Ok(permit)) => {
                permit.forget();
                Some(Token::new(&self.inner))
            }
            Err(_) => None,

            Ok(Err(_)) => {
                panic!("we own the semaphore, we shouldn't have closed it")
            }
        }
    }

    /// Return the concurrency [Token], along with the outcome of the job.
    ///
    /// The [Outcome] of the job, and the time taken to perform it, may be used
    /// to update the concurrency limit.
    ///
    /// Set the outcome to `None` to ignore the job.
    pub async fn release(&self, token: Token<'_>, outcome: Option<Outcome>) {
        if let Some(outcome) = outcome {
            let sample = Sample {
                latency: token.start.elapsed(),
                in_flight: self.in_flight(),
                outcome,
            };

            let new_limit = self.limit_algo.update(sample).await;

            let old_limit = self.inner.limit.swap(new_limit, Ordering::SeqCst);

            match new_limit.cmp(&old_limit) {
                cmp::Ordering::Greater => {
                    self.inner.semaphore.add_permits(new_limit - old_limit);

                    #[cfg(test)]
                    if let Some(n) = &self.inner.notifier {
                        n.notify_one();
                    }
                }
                cmp::Ordering::Less => {
                    let semaphore = self.inner.semaphore.clone();
                    #[cfg(test)]
                    let notifier = self.inner.notifier.clone();

                    tokio::spawn(async move {
                        // If there aren't enough permits available then this will wait until enough
                        // become available. This could take a while, so we do this in the background.
                        let permits = semaphore
                            .acquire_many((old_limit - new_limit) as u32)
                            .await
                            .expect("we own the semaphore, we shouldn't have closed it");

                        // Acquiring some permits and throwing them away reduces the available limit.
                        permits.forget();

                        #[cfg(test)]
                        if let Some(n) = notifier {
                            n.notify_one();
                        }
                    });
                }
                _ =>
                {
                    #[cfg(test)]
                    if let Some(n) = &self.inner.notifier {
                        n.notify_one();
                    }
                }
            }
        }

        drop(token);
    }

    pub(crate) fn available(&self) -> usize {
        self.inner.semaphore.available_permits()
    }

    pub(crate) fn limit(&self) -> usize {
        self.inner.limit.load(Ordering::Acquire)
    }

    pub(crate) fn in_flight(&self) -> usize {
        self.inner.in_flight.load(Ordering::Acquire)
    }

    /// The current state of the limiter.
    pub fn state(&self) -> LimiterState {
        LimiterState {
            limit: self.limit(),
            available: self.available(),
            in_flight: self.in_flight(),
        }
    }
}

impl<'t> Token<'t> {
    fn new(limiter: &'t LimiterInner) -> Self {
        Self {
            limiter,
            start: Instant::now(),
        }
    }

    #[cfg(test)]
    pub fn set_latency(&mut self, latency: Duration) {
        use std::ops::Sub;

        self.start = Instant::now().sub(latency);
    }
}

impl Drop for Token<'_> {
    /// Reduces the number of jobs in flight.
    fn drop(&mut self) {
        self.limiter.semaphore.add_permits(1);
        self.limiter.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl LimiterState {
    /// The current concurrency limit.
    pub fn limit(&self) -> usize {
        self.limit
    }
    /// The amount of concurrency available to use.
    pub fn available(&self) -> usize {
        self.available
    }
    /// The number of jobs in flight.
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }
}

impl Outcome {
    pub(crate) fn overloaded_or(self, other: Outcome) -> Outcome {
        use Outcome::*;
        match (self, other) {
            (Success, Overload) => Overload,
            _ => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{limits::Fixed, Limiter, Outcome};

    #[tokio::test]
    async fn it_works() {
        let limiter = Limiter::new(Fixed::new(10));

        let token = limiter.try_acquire().unwrap();

        limiter.release(token, Some(Outcome::Success)).await;

        assert_eq!(limiter.limit(), 10);
    }
}
