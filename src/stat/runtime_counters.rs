use std::sync::atomic::{AtomicU64, Ordering};

static MONITOR_SEND_DROP_FULL: AtomicU64 = AtomicU64::new(0);
static MONITOR_SEND_DROP_CLOSED: AtomicU64 = AtomicU64::new(0);
static SINK_CHANNEL_FULL: AtomicU64 = AtomicU64::new(0);
static SINK_CHANNEL_CLOSED: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default)]
pub struct RuntimeCounterSnapshot {
    pub monitor_send_drop_full: u64,
    pub monitor_send_drop_closed: u64,
    pub sink_channel_full: u64,
    pub sink_channel_closed: u64,
}

impl RuntimeCounterSnapshot {
    pub fn is_empty(&self) -> bool {
        self.monitor_send_drop_full == 0
            && self.monitor_send_drop_closed == 0
            && self.sink_channel_full == 0
            && self.sink_channel_closed == 0
    }
}

pub fn rec_monitor_send_drop_full() {
    MONITOR_SEND_DROP_FULL.fetch_add(1, Ordering::Relaxed);
}

pub fn rec_monitor_send_drop_closed() {
    MONITOR_SEND_DROP_CLOSED.fetch_add(1, Ordering::Relaxed);
}

pub fn rec_sink_channel_full() {
    SINK_CHANNEL_FULL.fetch_add(1, Ordering::Relaxed);
}

pub fn rec_sink_channel_closed() {
    SINK_CHANNEL_CLOSED.fetch_add(1, Ordering::Relaxed);
}

pub fn take_snapshot() -> RuntimeCounterSnapshot {
    RuntimeCounterSnapshot {
        monitor_send_drop_full: MONITOR_SEND_DROP_FULL.swap(0, Ordering::Relaxed),
        monitor_send_drop_closed: MONITOR_SEND_DROP_CLOSED.swap(0, Ordering::Relaxed),
        sink_channel_full: SINK_CHANNEL_FULL.swap(0, Ordering::Relaxed),
        sink_channel_closed: SINK_CHANNEL_CLOSED.swap(0, Ordering::Relaxed),
    }
}
