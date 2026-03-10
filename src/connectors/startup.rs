//! Centralized initialization for engine-side connector registries.
//! - Registers built-in sinks
//! - Registers built-in sources (syslog, tcp, file)
//! - Imports any factories that were (still) registered via API registries
//! - Logs the final registered kinds for diagnostics

pub fn init_runtime_registries() {
    wp_core_connectors::startup::init_runtime_registries(
        crate::sinks::register_builtin_factories,
        || {
            crate::sources::syslog::register_syslog_factory();
            crate::sources::tcp::register_tcp_factory();
            crate::sources::file::register_factory_only();
        },
    );
}

pub fn log_registered_kinds() {
    wp_core_connectors::startup::log_registered_kinds();
}
