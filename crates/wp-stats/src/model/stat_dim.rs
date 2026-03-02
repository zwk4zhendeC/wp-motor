use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

use crate::model::dimension::{DataDim, StatTarget};
use smol_str::SmolStr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StatDim {
    target: Option<SmolStr>,
    collect: DataDim,
}

impl StatDim {
    pub fn dat_string(&self) -> &DataDim {
        &self.collect
    }

    /// Returns the optional target string associated with this dimension.
    ///
    /// When the `StatTarget` is `All`, this is `Some(real_target)`; when the
    /// target is `Ignore` or a non-matching `Item`, this is `None`.
    pub fn target_str(&self) -> Option<&str> {
        self.target.as_deref()
    }
}

impl PartialOrd<Self> for StatDim {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StatDim {
    fn cmp(&self, other: &Self) -> Ordering {
        let order = self.target.cmp(&other.target);
        if order == Ordering::Equal {
            return self.collect.cmp(&other.collect);
        }
        order
    }
}

impl Display for StatDim {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}@{}",
            self.target.as_deref().unwrap_or("*"),
            self.collect,
        )
    }
}
pub trait DimensionBuilder<T> {
    fn make_dim(req_target: &StatTarget, target_str: &str, data: T) -> Self;
}
impl DimensionBuilder<DataDim> for StatDim {
    fn make_dim(req_target: &StatTarget, real_target: &str, data: DataDim) -> Self {
        let target = Self::make_rule_value(req_target, real_target);
        StatDim {
            target,
            collect: data,
        }
    }
}

impl DimensionBuilder<()> for StatDim {
    fn make_dim(req_target: &StatTarget, real_target: &str, _: ()) -> Self {
        let rule_dim = Self::make_rule_value(req_target, real_target);
        StatDim {
            target: rule_dim,
            collect: DataDim::empty(),
        }
    }
}

impl StatDim {
    fn make_rule_value(req_target: &StatTarget, target_str: &str) -> Option<SmolStr> {
        match req_target {
            StatTarget::All => Some(SmolStr::new(target_str)),
            StatTarget::Ignore => None,
            StatTarget::Item(item) => {
                if item == target_str {
                    Some(SmolStr::new(item))
                } else {
                    None
                }
            }
        }
    }
}
