use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::{DomainError, ErrorCode, Result};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_delay_seconds: u64,
    pub max_delay_seconds: u64,
    pub multiplier: u32,
}

impl RetryPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.max_attempts == 0 {
            return Err(DomainError::validation(
                ErrorCode::InvalidRetryPolicy,
                "retry.max_attempts",
                "must be at least one",
            ));
        }
        if self.multiplier == 0 {
            return Err(DomainError::validation(
                ErrorCode::InvalidRetryPolicy,
                "retry.multiplier",
                "must be at least one",
            ));
        }
        if self.initial_delay_seconds > self.max_delay_seconds {
            return Err(DomainError::validation(
                ErrorCode::InvalidRetryPolicy,
                "retry.initial_delay_seconds",
                "must not exceed max_delay_seconds",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn delay_after_failure(&self, attempts: u32) -> Option<Duration> {
        if attempts >= self.max_attempts {
            return None;
        }
        let mut delay = self.initial_delay_seconds.min(self.max_delay_seconds);
        for _ in 1..attempts {
            delay = delay
                .saturating_mul(u64::from(self.multiplier))
                .min(self.max_delay_seconds);
            if delay == self.max_delay_seconds {
                break;
            }
        }
        Some(Duration::from_secs(delay))
    }
}

macro_rules! retry_policy {
    ($name:ident, $attempts:expr, $initial:expr, $maximum:expr, $multiplier:expr) => {
        #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
        #[serde(transparent)]
        pub struct $name(pub RetryPolicy);

        impl Default for $name {
            fn default() -> Self {
                Self(RetryPolicy {
                    max_attempts: $attempts,
                    initial_delay_seconds: $initial,
                    max_delay_seconds: $maximum,
                    multiplier: $multiplier,
                })
            }
        }
    };
}

retry_policy!(AttemptRetryPolicy, 1, 0, 0, 1);
retry_policy!(ActionRetryPolicy, 3, 5, 300, 2);
retry_policy!(DeliveryRetryPolicy, 8, 10, 3600, 2);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_is_bounded_and_stops_at_attempt_limit() {
        let policy = RetryPolicy {
            max_attempts: 6,
            initial_delay_seconds: 5,
            max_delay_seconds: 30,
            multiplier: u32::MAX,
        };
        assert_eq!(policy.delay_after_failure(1), Some(Duration::from_secs(5)));
        assert_eq!(policy.delay_after_failure(2), Some(Duration::from_secs(30)));
        assert_eq!(policy.delay_after_failure(5), Some(Duration::from_secs(30)));
        assert_eq!(policy.delay_after_failure(6), None);
    }
}
