use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{process, time::Duration};
use uuid::Uuid;

static EVENT_ID_SEED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(init_event_id_seed()));

/// 全局事件 ID 生成器：返回单调递增的 u64。
pub fn next_event_id() -> u64 {
    EVENT_ID_SEED.fetch_add(1, Ordering::Relaxed)
}

fn init_event_id_seed() -> u64 {
    compose_event_id_seed(
        SystemTime::now().duration_since(UNIX_EPOCH).ok(),
        process::id(),
        uuid_entropy(),
    )
}

fn compose_event_id_seed(time_since_epoch: Option<Duration>, pid: u32, entropy: u64) -> u64 {
    let time_nanos = time_since_epoch.map(duration_to_u64_nanos).unwrap_or(0);
    let pid_bits = u64::from(pid).rotate_left(32);

    let mut seed = entropy ^ time_nanos.rotate_left(13) ^ pid_bits ^ 0x9E37_79B9_7F4A_7C15;

    seed ^= seed >> 33;
    seed = seed.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    seed ^= seed >> 33;
    seed = seed.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    seed ^= seed >> 33;

    if seed == 0 { 1 } else { seed }
}

fn duration_to_u64_nanos(duration: Duration) -> u64 {
    duration.as_nanos() as u64
}

fn uuid_entropy() -> u64 {
    let bytes = Uuid::new_v4().into_bytes();
    let hi = u64::from_le_bytes(bytes[..8].try_into().expect("uuid hi bytes"));
    let lo = u64::from_le_bytes(bytes[8..].try_into().expect("uuid lo bytes"));
    hi ^ lo.rotate_left(17)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn compose_event_id_seed_is_non_zero_when_time_is_unavailable() {
        let seed = compose_event_id_seed(None, 42, 0x1234_5678_9ABC_DEF0);
        assert_ne!(seed, 0);
    }

    #[test]
    fn compose_event_id_seed_changes_across_restarts_even_without_time() {
        let first = compose_event_id_seed(None, 42, 0x1111_2222_3333_4444);
        let second = compose_event_id_seed(None, 42, 0x5555_6666_7777_8888);
        assert_ne!(first, second);
    }

    #[test]
    fn compose_event_id_seed_changes_with_same_entropy_but_different_time() {
        let first = compose_event_id_seed(Some(Duration::from_secs(1)), 42, 7);
        let second = compose_event_id_seed(Some(Duration::from_secs(2)), 42, 7);
        assert_ne!(first, second);
    }

    #[test]
    #[serial]
    fn next_event_id_is_monotonic_within_one_process() {
        let first = next_event_id();
        let second = next_event_id();
        assert_eq!(second, first + 1);
    }
}
