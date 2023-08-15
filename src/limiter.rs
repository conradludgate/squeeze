use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    task::{self, Poll},
    time::Duration,
};

use pin_list::PinList;
use pin_project_lite::pin_project;
use tokio::time::{timeout, Instant};

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
    limit_algo: Mutex<T>,
    inner: LimiterInner,
}

type SemaphoreTypes = dyn pin_list::Types<
    Id = pin_list::id::Checked,
    Protected = std::task::Waker,
    Removed = (),
    Unprotected = (),
>;

#[derive(Debug)]
struct LimiterInner {
    semaphore2: Mutex<PinList<SemaphoreTypes>>,

    // first 32 bits are the limit, second 32 bits are the in-flight
    limits: AtomicU64,

    #[cfg(test)]
    notifier: Option<std::sync::Arc<tokio::sync::Notify>>,
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
    limit: u32,
    available: u32,
    in_flight: u32,
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
    pub fn new(limit_algo: T, initial_limit: u32) -> Self {
        assert!(initial_limit > 0);
        Self {
            limit_algo: Mutex::new(limit_algo),
            inner: LimiterInner {
                semaphore2: Mutex::new(PinList::new(pin_list::id::Checked::new())),
                limits: AtomicU64::new((initial_limit as u64) << 32),

                #[cfg(test)]
                notifier: None,
            },
        }
    }

    /// In some cases [Token]s are acquired asynchronously when updating the limit.
    #[cfg(test)]
    pub fn with_release_notifier(mut self, n: std::sync::Arc<tokio::sync::Notify>) -> Self {
        self.inner.notifier = Some(n);
        self
    }

    /// Try to immediately acquire a concurrency [Token].
    ///
    /// Returns `None` if there are none available.
    pub fn try_acquire(&self) -> Option<Token<'_>> {
        let mut curr = self.inner.limits.load(Ordering::Acquire);
        loop {
            let limit = (curr >> 32) as u32;
            let in_flight = curr as u32;
            if in_flight >= limit {
                return None;
            }

            match self.inner.limits.compare_exchange(
                curr,
                curr + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(Token::new(&self.inner));
                }
                Err(actual) => curr = actual,
            }
        }
    }

    /// Try to acquire a concurrency [Token], waiting for `duration` if there are none available.
    ///
    /// Returns `None` if there are none available after `duration`.
    pub async fn acquire_timeout(&self, duration: Duration) -> Option<Token<'_>> {
        match timeout(
            duration,
            Acquire {
                semaphore: &self.inner,
                node: pin_list::Node::new(),
            },
        )
        .await
        {
            Ok(permit) => Some(permit),
            Err(_) => None,
        }
    }

    /// Return the concurrency [Token], along with the outcome of the job.
    ///
    /// The [Outcome] of the job, and the time taken to perform it, may be used
    /// to update the concurrency limit.
    ///
    /// Set the outcome to `None` to ignore the job.
    pub async fn release(&self, token: Token<'_>, outcome: Option<Outcome>) {
        let (new_limit, old_limit) = if let Some(outcome) = outcome {
            let mut state = self.inner.limits.load(Ordering::Acquire);

            loop {
                let old_limit = (state >> 32) as u32;
                let in_flight = state as u32;

                let sample = Sample {
                    latency: token.start.elapsed(),
                    in_flight,
                    outcome,
                };

                let alg = self.limit_algo.lock().unwrap().clone();
                let (alg, new_limit) = alg.update(old_limit, sample).await;

                match self.inner.limits.compare_exchange(
                    state,
                    (new_limit as u64) << 32 | (state & 0xffffffff),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        *self.limit_algo.lock().unwrap() = alg;
                        break (new_limit, old_limit);
                    }
                    Err(s) => state = s,
                }
            }
        } else {
            (0, 0)
        };
        std::mem::forget(token);

        if new_limit >= old_limit {
            self.inner.release((new_limit - old_limit) as usize);
        }

        #[cfg(test)]
        if let Some(n) = &self.inner.notifier {
            n.notify_one();
        }
    }

    /// The current state of the limiter.
    pub fn state(&self) -> LimiterState {
        let state = self.inner.limits.load(Ordering::Relaxed);
        let limit = (state >> 32) as u32;
        let in_flight = state as u32;
        LimiterState {
            limit,
            available: limit.saturating_sub(in_flight),
            in_flight,
        }
    }
}

impl LimiterInner {
    fn release(&self, new_permits: usize) {
        // TODO: use array vec like tokio
        // +1 because we are also releasing a single token
        let mut wakers = Vec::with_capacity(new_permits + 1);
        {
            let mut semaphore = self.semaphore2.lock().unwrap();
            let mut cursor = semaphore.cursor_front_mut();
            while wakers.len() < wakers.capacity() {
                if let Ok(waker) = cursor.remove_current(()) {
                    wakers.push(waker);
                } else {
                    break;
                }
            }
            // do this while locked
            if wakers.len() > 1 {
                // mark the permits as taken
                self.limits
                    .fetch_add(wakers.len() as u64 - 1, Ordering::Release);
            } else if wakers.is_empty() {
                // 1 permite has been released
                self.limits.fetch_sub(1, Ordering::Release);
            }
        }
        for waker in wakers {
            waker.wake()
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
        self.limiter.release(1);
    }
}

impl LimiterState {
    /// The current concurrency limit.
    pub fn limit(&self) -> u32 {
        self.limit
    }
    /// The amount of concurrency available to use.
    pub fn available(&self) -> u32 {
        self.available
    }
    /// The number of jobs in flight.
    pub fn in_flight(&self) -> u32 {
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

pin_project! {
    struct Acquire<'s> {
        semaphore: &'s LimiterInner,
        #[pin]
        node: pin_list::Node<SemaphoreTypes>,
    }

    impl PinnedDrop for Acquire<'_> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            let node = match this.node.initialized_mut() {
                // The future was cancelled before it could complete.
                Some(initialized) => initialized,
                // The future has completed already (or hasn't started); we don't have to do
                // anything.
                None => return,
            };

            let mut inner = this.semaphore.semaphore2.lock().unwrap();

            match node.reset(&mut inner) {
                // If we've cancelled the future like usual, just do that.
                (pin_list::NodeData::Linked(_waker), ()) => {}

                // Otherwise, we have been woken but aren't around to take the lock. To
                // prevent deadlocks, pass the notification on to someone else.
                (pin_list::NodeData::Removed(()), ()) => {
                    if let Ok(waker) = inner.cursor_front_mut().remove_current(()) {
                        drop(inner);
                        waker.wake();
                    }
                }
            }
        }
    }
}

impl<'s> Future for Acquire<'s> {
    type Output = Token<'s>;
    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        let mut inner = this.semaphore.semaphore2.lock().unwrap();

        if let Some(node) = this.node.as_mut().initialized_mut() {
            // Check whether we've been woken up, only continuing if so.
            if let Err(node) = node.take_removed(&inner) {
                // If we haven't been woken, re-register our waker and pend.
                *node.protected_mut(&mut inner).unwrap() = cx.waker().clone();
                return Poll::Pending;
            }
        }

        // Otherwise, re-register ourselves to be woken when the mutex is unlocked again
        inner.push_back(this.node, cx.waker().clone(), ());

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use crate::{limits::Fixed, Limiter, Outcome};

    #[tokio::test]
    async fn it_works() {
        let limiter = Limiter::new(Fixed, 10);

        let token = limiter.try_acquire().unwrap();

        limiter.release(token, Some(Outcome::Success)).await;

        assert_eq!(limiter.state().limit(), 10);
    }
}
