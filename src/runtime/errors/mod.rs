use orion_error::UvsReason;
use wp_connector_api::{SinkError, SinkReason};
use wp_connector_api::{SourceError, SourceReason};
use wp_error::error_handling::ErrorHandlingStrategy;
use wp_error::error_handling::RobustnessMode;
use wpl::{WparseError, WparseReason};

// 运行时错误策略映射：统一在 runtime/errors 下维护
// - sink 写入错误 → 重试/容错/终止策略
// - 解析错误 → 忽略/容错/终止策略
// - 源派发错误 → 容忍/重试/终止/抛出

pub fn err4_send_to_sink(err: &SinkError, mode: &RobustnessMode) -> ErrorHandlingStrategy {
    match err.reason() {
        SinkReason::Sink(e) => {
            warn_data!("sink error: {}", e);
            ErrorHandlingStrategy::FixRetry
        }
        SinkReason::Mock => {
            info_data!("mock ");
            ErrorHandlingStrategy::FixRetry
        }
        SinkReason::StgCtrl => {
            info_data!("stg ctrl");
            ErrorHandlingStrategy::FixRetry
        }
        SinkReason::Uvs(e) => universal_proc_stg(mode, e),
    }
}

pub fn err4_engine_parse_data(err: &WparseError, mode: &RobustnessMode) -> ErrorHandlingStrategy {
    match err.reason() {
        WparseReason::Plugin(_) => ErrorHandlingStrategy::Ignore,
        WparseReason::LineProc(_) => ErrorHandlingStrategy::Ignore,
        WparseReason::NotMatch => ErrorHandlingStrategy::Ignore,
        WparseReason::Uvs(e) => universal_proc_stg(mode, e),
    }
}

pub fn err4_dispatch_data(err: &SourceError, mode: &RobustnessMode) -> ErrorHandlingStrategy {
    match err.reason() {
        SourceReason::SupplierError(e) => {
            warn_data!("{}", e);
            ErrorHandlingStrategy::Throw
        }
        SourceReason::NotData => ErrorHandlingStrategy::Tolerant,
        SourceReason::EOF => ErrorHandlingStrategy::Terminate,
        SourceReason::Disconnect(e) => {
            warn_data!("rule error: {}", e);
            ErrorHandlingStrategy::FixRetry
        }
        SourceReason::Other(e) => {
            error_data!("other error: {}", e);
            ErrorHandlingStrategy::Throw
        }
        SourceReason::Uvs(e) => universal_proc_stg(mode, e),
    }
}

fn universal_proc_stg(mode: &RobustnessMode, e: &UvsReason) -> ErrorHandlingStrategy {
    match e {
        UvsReason::ValidationError => {
            error_data!("validation error");
            ErrorHandlingStrategy::Throw
        }
        UvsReason::LogicError => match mode {
            RobustnessMode::Strict => {
                error_data!("logic error");
                ErrorHandlingStrategy::Tolerant
            }
            _ => {
                error_data!("logic error");
                ErrorHandlingStrategy::Throw
            }
        },
        UvsReason::DataError => ErrorHandlingStrategy::Tolerant,
        UvsReason::SystemError => {
            warn_data!("system error");
            ErrorHandlingStrategy::Tolerant
        }
        UvsReason::BusinessError => {
            warn_data!("biz error");
            ErrorHandlingStrategy::Tolerant
        }
        UvsReason::RunRuleError => {
            warn_data!("run rule error");
            ErrorHandlingStrategy::Throw
        }
        UvsReason::NotFoundError => {
            error_data!("not found error");
            ErrorHandlingStrategy::Throw
        }
        UvsReason::PermissionError => {
            error_data!("permission error");
            ErrorHandlingStrategy::Throw
        }
        UvsReason::NetworkError => {
            warn_data!("network error");
            ErrorHandlingStrategy::Throw
        }
        UvsReason::ResourceError => {
            error_data!("resource error");
            ErrorHandlingStrategy::Throw
        }
        UvsReason::TimeoutError => {
            warn_data!("timeout error");
            ErrorHandlingStrategy::Throw
        }
        UvsReason::ConfigError(e) => {
            error_data!("conf error: {}", e);
            ErrorHandlingStrategy::Throw
        }
        UvsReason::ExternalError => {
            error_data!("external error");
            ErrorHandlingStrategy::Throw
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_error_does_not_panic_and_throws() {
        let err = SourceError::from(SourceReason::Uvs(UvsReason::ResourceError));
        let stg = err4_dispatch_data(&err, &RobustnessMode::Debug);
        assert!(matches!(stg, ErrorHandlingStrategy::Throw));
    }
}
