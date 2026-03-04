use crate::runtime::actor::TaskGroup;
use crate::runtime::actor::command::spawn_ctrl_event_bridge;
use crate::runtime::actor::signal::ShutdownCmd;
use wp_connector_api::AcceptorHandle;

/// 启动独立接受器任务组（acceptors）。
///
/// 该组与 picker 分离，便于在退出策略中独立判定 acceptor 生命周期。
pub fn start_acceptor_tasks(all_acceptors: Vec<AcceptorHandle>) -> TaskGroup {
    let mut group = TaskGroup::new("acceptor", ShutdownCmd::Immediate);
    info_ctrl!("启动接受器: {}个", all_acceptors.len());
    for handle in all_acceptors {
        let mut acceptor = handle.acceptor;
        let cmd_sub = group.subscribe();
        group.append(tokio::spawn(async move {
            info_ctrl!("启动接受器任务");
            let ctrl_rx = spawn_ctrl_event_bridge(cmd_sub.clone(), 1024);

            if let Err(e) = acceptor.accept_connection(ctrl_rx).await {
                error_ctrl!("接受器错误: {}", e);
            } else {
                info_ctrl!("接受器正常结束");
            }
        }));
    }
    group
}
