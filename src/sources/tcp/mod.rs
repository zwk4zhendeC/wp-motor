//! TCP 源实现 - 高性能分离式架构
//!
//! 模块结构：
//! - framing.rs：TCP消息分帧处理（行/长度前缀/自动检测）
//! - source.rs：TcpSource 实现，直接管理监听 loop 交付的 sockets
//! - zc_types.rs：零拷贝数据结构

pub mod acceptor {
    pub use wp_core_connectors::sources::tcp::acceptor::*;
}

pub mod config {
    pub use wp_core_connectors::sources::tcp::config::*;
}

pub mod conn {
    pub mod connection {
        pub use wp_core_connectors::sources::tcp::conn::connection::*;
    }
}

pub mod factory {
    pub use wp_core_connectors::sources::tcp::factory::*;
}

pub mod framing {
    pub use wp_core_connectors::sources::tcp::framing::*;
}

pub mod source {
    pub use wp_core_connectors::sources::tcp::source::*;
}

pub mod worker {
    pub use wp_core_connectors::sources::tcp::worker::*;
}

pub mod zc {
    pub mod types {
        pub use wp_core_connectors::sources::tcp::zc::types::*;
    }
}

pub use wp_core_connectors::sources::tcp::{
    BatchConfig, BufferPoolMetrics, BufferStats, FramingMode, MessageBatch, TcpAcceptor, TcpSource,
    TcpSourceFactory, ZcpConfig, ZcpMessage, ZcpResult, ZeroCopyError, register_tcp_factory,
};
