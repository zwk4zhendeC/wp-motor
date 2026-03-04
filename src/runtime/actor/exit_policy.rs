use crate::runtime::actor::signal::ShutdownCmd;
use std::collections::BTreeMap;
use std::time::Duration;

#[cfg(test)]
const DAEMON_QUIESCING_MAX_WAIT: Duration = Duration::from_millis(200);
#[cfg(not(test))]
const DAEMON_QUIESCING_MAX_WAIT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TaskRole {
    Monitor,
    Infra,
    Sink,
    Maintainer,
    Parser,
    Picker,
    Acceptor,
    Generator,
    Router,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitPolicyKind {
    Batch,
    Daemon,
    Generator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitPhase {
    Running,
    Quiescing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitTrigger {
    Signal,
    RoleFinished(TaskRole),
    QuiescingTimeout,
}

#[derive(Debug, Clone)]
pub struct RuntimeSnapshot {
    phase: ExitPhase,
    phase_elapsed: Duration,
    signal_received: bool,
    role_totals: BTreeMap<TaskRole, usize>,
    role_finished: BTreeMap<TaskRole, bool>,
}

impl RuntimeSnapshot {
    pub fn new(
        phase: ExitPhase,
        phase_elapsed: Duration,
        signal_received: bool,
        role_totals: BTreeMap<TaskRole, usize>,
        role_finished: BTreeMap<TaskRole, bool>,
    ) -> Self {
        Self {
            phase,
            phase_elapsed,
            signal_received,
            role_totals,
            role_finished,
        }
    }

    pub fn phase(&self) -> ExitPhase {
        self.phase
    }

    pub fn signal_received(&self) -> bool {
        self.signal_received
    }

    pub fn phase_elapsed(&self) -> Duration {
        self.phase_elapsed
    }

    pub fn role_finished(&self, role: TaskRole) -> bool {
        self.role_finished.get(&role).copied().unwrap_or(false)
    }

    pub fn has_role(&self, role: TaskRole) -> bool {
        self.role_totals.get(&role).copied().unwrap_or(0) > 0
    }

    pub fn role_finished_or_absent(&self, role: TaskRole) -> bool {
        !self.has_role(role) || self.role_finished(role)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitAction {
    Stay,
    EnterQuiescing(ExitTrigger),
    EnterStopping(ExitTrigger),
}

pub trait ExitPolicy {
    fn decide(&mut self, snapshot: &RuntimeSnapshot) -> ExitAction;

    fn stop_cmd(&self) -> ShutdownCmd {
        ShutdownCmd::Immediate
    }
}

#[derive(Default)]
pub struct BatchExitPolicy;

impl ExitPolicy for BatchExitPolicy {
    fn decide(&mut self, snapshot: &RuntimeSnapshot) -> ExitAction {
        match snapshot.phase() {
            ExitPhase::Running => {
                if snapshot.signal_received() {
                    return ExitAction::EnterStopping(ExitTrigger::Signal);
                }
                if snapshot.role_finished(TaskRole::Picker) {
                    return ExitAction::EnterQuiescing(ExitTrigger::RoleFinished(TaskRole::Picker));
                }
                ExitAction::Stay
            }
            ExitPhase::Quiescing => {
                if snapshot.signal_received() {
                    return ExitAction::EnterStopping(ExitTrigger::Signal);
                }
                if snapshot.role_finished(TaskRole::Parser) {
                    return ExitAction::EnterStopping(ExitTrigger::RoleFinished(TaskRole::Parser));
                }
                ExitAction::Stay
            }
        }
    }
}

#[derive(Default)]
pub struct DaemonExitPolicy;

impl ExitPolicy for DaemonExitPolicy {
    fn decide(&mut self, snapshot: &RuntimeSnapshot) -> ExitAction {
        match snapshot.phase() {
            ExitPhase::Running => {
                if snapshot.signal_received() {
                    return ExitAction::EnterStopping(ExitTrigger::Signal);
                }
                if snapshot.role_finished(TaskRole::Picker) {
                    if snapshot.role_finished_or_absent(TaskRole::Acceptor) {
                        return ExitAction::EnterStopping(ExitTrigger::RoleFinished(
                            TaskRole::Picker,
                        ));
                    }
                    return ExitAction::EnterQuiescing(ExitTrigger::RoleFinished(TaskRole::Picker));
                }
                ExitAction::Stay
            }
            ExitPhase::Quiescing => {
                if snapshot.signal_received() {
                    return ExitAction::EnterStopping(ExitTrigger::Signal);
                }
                if snapshot.role_finished_or_absent(TaskRole::Acceptor) {
                    return ExitAction::EnterStopping(ExitTrigger::RoleFinished(TaskRole::Picker));
                }
                if snapshot.phase_elapsed() >= DAEMON_QUIESCING_MAX_WAIT {
                    return ExitAction::EnterStopping(ExitTrigger::QuiescingTimeout);
                }
                ExitAction::Stay
            }
        }
    }
}

#[derive(Default)]
pub struct GeneratorExitPolicy;

impl ExitPolicy for GeneratorExitPolicy {
    fn decide(&mut self, snapshot: &RuntimeSnapshot) -> ExitAction {
        match snapshot.phase() {
            ExitPhase::Running | ExitPhase::Quiescing => {
                if snapshot.signal_received() {
                    return ExitAction::EnterStopping(ExitTrigger::Signal);
                }
                if snapshot.role_finished(TaskRole::Generator) {
                    return ExitAction::EnterStopping(ExitTrigger::RoleFinished(
                        TaskRole::Generator,
                    ));
                }
                ExitAction::Stay
            }
        }
    }
}

pub fn build_exit_policy(kind: ExitPolicyKind) -> Box<dyn ExitPolicy + Send> {
    match kind {
        ExitPolicyKind::Batch => Box::new(BatchExitPolicy),
        ExitPolicyKind::Daemon => Box::new(DaemonExitPolicy),
        ExitPolicyKind::Generator => Box::new(GeneratorExitPolicy),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(
        phase: ExitPhase,
        signal_received: bool,
        role_totals: &[(TaskRole, usize)],
        finished_roles: &[(TaskRole, bool)],
    ) -> RuntimeSnapshot {
        snapshot_with_elapsed(
            phase,
            Duration::from_millis(0),
            signal_received,
            role_totals,
            finished_roles,
        )
    }

    fn snapshot_with_elapsed(
        phase: ExitPhase,
        phase_elapsed: Duration,
        signal_received: bool,
        role_totals: &[(TaskRole, usize)],
        finished_roles: &[(TaskRole, bool)],
    ) -> RuntimeSnapshot {
        let mut totals = BTreeMap::new();
        for (role, total) in role_totals {
            totals.insert(*role, *total);
        }
        let mut role_finished = BTreeMap::new();
        for (role, finished) in finished_roles {
            role_finished.insert(*role, *finished);
        }
        RuntimeSnapshot::new(phase, phase_elapsed, signal_received, totals, role_finished)
    }

    #[test]
    fn batch_policy_transitions_running_to_quiescing_on_picker_finished() {
        let mut policy = BatchExitPolicy;
        let snap = snapshot(
            ExitPhase::Running,
            false,
            &[(TaskRole::Picker, 1), (TaskRole::Parser, 1)],
            &[(TaskRole::Picker, true), (TaskRole::Parser, false)],
        );
        assert_eq!(
            policy.decide(&snap),
            ExitAction::EnterQuiescing(ExitTrigger::RoleFinished(TaskRole::Picker))
        );
    }

    #[test]
    fn batch_policy_stops_on_parser_finished_after_quiescing() {
        let mut policy = BatchExitPolicy;
        let snap = snapshot(
            ExitPhase::Quiescing,
            false,
            &[(TaskRole::Picker, 1), (TaskRole::Parser, 1)],
            &[(TaskRole::Picker, true), (TaskRole::Parser, true)],
        );
        assert_eq!(
            policy.decide(&snap),
            ExitAction::EnterStopping(ExitTrigger::RoleFinished(TaskRole::Parser))
        );
    }

    #[test]
    fn batch_policy_signal_has_highest_priority() {
        let mut policy = BatchExitPolicy;
        let snap = snapshot(
            ExitPhase::Running,
            true,
            &[(TaskRole::Picker, 1), (TaskRole::Parser, 1)],
            &[(TaskRole::Picker, true), (TaskRole::Parser, true)],
        );
        assert_eq!(
            policy.decide(&snap),
            ExitAction::EnterStopping(ExitTrigger::Signal)
        );
    }

    #[test]
    fn daemon_policy_stops_when_picker_finished() {
        let mut policy = DaemonExitPolicy;
        let snap = snapshot(
            ExitPhase::Running,
            false,
            &[(TaskRole::Picker, 1)],
            &[(TaskRole::Picker, true), (TaskRole::Parser, false)],
        );
        assert_eq!(
            policy.decide(&snap),
            ExitAction::EnterStopping(ExitTrigger::RoleFinished(TaskRole::Picker))
        );
    }

    #[test]
    fn daemon_policy_keeps_running_when_no_signal_and_picker_alive() {
        let mut policy = DaemonExitPolicy;
        let snap = snapshot(
            ExitPhase::Running,
            false,
            &[(TaskRole::Picker, 1)],
            &[(TaskRole::Picker, false), (TaskRole::Parser, false)],
        );
        assert_eq!(policy.decide(&snap), ExitAction::Stay);
    }

    #[test]
    fn builder_returns_policies_with_expected_behavior() {
        let mut batch = build_exit_policy(ExitPolicyKind::Batch);
        let mut daemon = build_exit_policy(ExitPolicyKind::Daemon);
        let running_picker_done = snapshot(
            ExitPhase::Running,
            false,
            &[(TaskRole::Picker, 1)],
            &[(TaskRole::Picker, true)],
        );
        assert_eq!(
            batch.decide(&running_picker_done),
            ExitAction::EnterQuiescing(ExitTrigger::RoleFinished(TaskRole::Picker))
        );
        assert_eq!(
            daemon.decide(&running_picker_done),
            ExitAction::EnterStopping(ExitTrigger::RoleFinished(TaskRole::Picker))
        );
    }

    #[test]
    fn daemon_policy_waits_acceptor_when_present() {
        let mut policy = DaemonExitPolicy;
        let snap = snapshot(
            ExitPhase::Running,
            false,
            &[(TaskRole::Picker, 1), (TaskRole::Acceptor, 1)],
            &[(TaskRole::Picker, true), (TaskRole::Acceptor, false)],
        );
        assert_eq!(
            policy.decide(&snap),
            ExitAction::EnterQuiescing(ExitTrigger::RoleFinished(TaskRole::Picker))
        );
    }

    #[test]
    fn daemon_policy_stops_when_picker_and_acceptor_both_finished() {
        let mut policy = DaemonExitPolicy;
        let snap = snapshot(
            ExitPhase::Running,
            false,
            &[(TaskRole::Picker, 1), (TaskRole::Acceptor, 1)],
            &[(TaskRole::Picker, true), (TaskRole::Acceptor, true)],
        );
        assert_eq!(
            policy.decide(&snap),
            ExitAction::EnterStopping(ExitTrigger::RoleFinished(TaskRole::Picker))
        );
    }

    #[test]
    fn daemon_policy_stops_from_quiescing_when_acceptor_done() {
        let mut policy = DaemonExitPolicy;
        let snap = snapshot(
            ExitPhase::Quiescing,
            false,
            &[(TaskRole::Picker, 1), (TaskRole::Acceptor, 1)],
            &[(TaskRole::Picker, true), (TaskRole::Acceptor, true)],
        );
        assert_eq!(
            policy.decide(&snap),
            ExitAction::EnterStopping(ExitTrigger::RoleFinished(TaskRole::Picker))
        );
    }

    #[test]
    fn daemon_policy_stops_from_quiescing_when_timeout_reached() {
        let mut policy = DaemonExitPolicy;
        let snap = snapshot_with_elapsed(
            ExitPhase::Quiescing,
            Duration::from_secs(31),
            false,
            &[(TaskRole::Picker, 1), (TaskRole::Acceptor, 1)],
            &[(TaskRole::Picker, true), (TaskRole::Acceptor, false)],
        );
        assert_eq!(
            policy.decide(&snap),
            ExitAction::EnterStopping(ExitTrigger::QuiescingTimeout)
        );
    }

    #[test]
    fn generator_policy_stops_when_generator_finished() {
        let mut policy = GeneratorExitPolicy;
        let snap = snapshot(
            ExitPhase::Running,
            false,
            &[(TaskRole::Generator, 1)],
            &[(TaskRole::Generator, true)],
        );
        assert_eq!(
            policy.decide(&snap),
            ExitAction::EnterStopping(ExitTrigger::RoleFinished(TaskRole::Generator))
        );
    }
}
