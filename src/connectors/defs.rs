use wp_conf::connectors::ConnectorDef;

pub fn builtin_sink_defs() -> Vec<ConnectorDef> {
    wp_core_connectors::builtin::builtin_sink_defs()
}

pub fn builtin_source_defs() -> Vec<ConnectorDef> {
    wp_core_connectors::builtin::builtin_source_defs()
}
