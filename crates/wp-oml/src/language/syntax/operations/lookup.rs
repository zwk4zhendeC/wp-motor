use crate::language::PreciseEvaluator;
use crate::language::prelude::*;
use derive_getters::Getters;
use std::collections::HashMap;
use std::sync::Arc;

pub type LookupDict = HashMap<String, Arc<DataField>>;

#[derive(Clone, Debug, Getters)]
pub struct LookupOperation {
    dict_symbol: String,
    key: Box<PreciseEvaluator>,
    default: Box<PreciseEvaluator>,
    compiled: Option<Arc<LookupDict>>,
}

impl LookupOperation {
    pub fn new(
        dict_symbol: impl Into<String>,
        key: PreciseEvaluator,
        default: PreciseEvaluator,
    ) -> Self {
        Self {
            dict_symbol: dict_symbol.into(),
            key: Box::new(key),
            default: Box::new(default),
            compiled: None,
        }
    }

    pub fn key_mut(&mut self) -> &mut PreciseEvaluator {
        self.key.as_mut()
    }

    pub fn default_mut(&mut self) -> &mut PreciseEvaluator {
        self.default.as_mut()
    }

    pub fn bind_compiled(&mut self, dict: LookupDict) {
        self.compiled = Some(Arc::new(dict));
    }
}

impl Display for LookupOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "lookup_nocase({}, {}, {})",
            self.dict_symbol, self.key, self.default
        )
    }
}
