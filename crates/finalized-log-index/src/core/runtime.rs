use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct RuntimeState {
    pub degraded: AtomicBool,
    pub throttled: AtomicBool,
    consecutive_backend_errors: AtomicU64,
    reason: Mutex<String>,
}

impl RuntimeState {
    pub fn set_degraded(&self, reason: impl Into<String>) {
        self.degraded.store(true, Ordering::Relaxed);
        self.throttled.store(false, Ordering::Relaxed);
        if let Ok(mut current_reason) = self.reason.lock() {
            *current_reason = reason.into();
        }
    }

    pub fn set_throttled(&self, reason: impl Into<String>) {
        self.throttled.store(true, Ordering::Relaxed);
        if let Ok(mut current_reason) = self.reason.lock() {
            *current_reason = reason.into();
        }
    }

    pub fn clear_throttle(&self) {
        self.throttled.store(false, Ordering::Relaxed);
        if !self.degraded.load(Ordering::Relaxed)
            && let Ok(mut current_reason) = self.reason.lock()
        {
            current_reason.clear();
        }
    }

    pub fn on_backend_success(&self) {
        self.consecutive_backend_errors.store(0, Ordering::Relaxed);
        if self.throttled.load(Ordering::Relaxed) && !self.degraded.load(Ordering::Relaxed) {
            self.clear_throttle();
        }
    }

    pub fn on_backend_error(&self, reason: String, throttle_after: u64, degraded_after: u64) {
        let count = self
            .consecutive_backend_errors
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if count >= degraded_after {
            self.set_degraded(format!(
                "backend failure threshold exceeded ({count}/{degraded_after}): {reason}"
            ));
        } else if count >= throttle_after {
            self.set_throttled(format!(
                "backend failures throttling ({count}/{throttle_after}): {reason}"
            ));
        }
    }

    pub fn reason(&self) -> String {
        self.reason
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_else(|_| "lock poisoned".to_string())
    }
}
